import logging
import os
import json
import datetime
import requests
from azure.storage.blob import BlobServiceClient 
import azure.functions as func 

# Azure Functions 앱 인스턴스 생성
app = func.FunctionApp()

# --- 웹훅 알림 함수 정의 (재사용성 및 간결성 확보) ---
def send_teams_webhook(message_text: str) -> None:
    """
    주어진 텍스트 메시지를 Teams 웹훅으로 전송하는 헬퍼 함수.
    local.settings.json의 'AzureWebHookUrl' 환경 변수를 사용
    """
    logging.info(f"Teams 웹훅 알림 요청: '{message_text[:50]}...'") 
    
    try:
        webhook_url = os.environ.get("AzureWebHookUrl") 
        if not webhook_url: # URL이 설정되지 않았거나 빈 문자열인 경우
            logging.warning("Teams 웹훅 URL이 환경 변수에 설정되어 있지 않습니다. 알림을 건너뜀.")
            return

        # Teams 웹훅 페이로드 구성
        webhook_payload = {
            "text": message_text # 받은 메시지 텍스트만 'text' 필드에 담아 보냄.
        }
        
        response = requests.post(webhook_url, json=webhook_payload)
        response.raise_for_status() # HTTP 응답 상태 코드 검사 
        logging.info(f"Teams 웹훅 실행 결과: {response.status_code}")
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Teams 웹훅 전송 중 요청 오류 발생: {e}")
    except Exception as e:
        logging.error(f"Teams 웹훅 페이로드 구성 또는 전송 중 오류 발생: {e}")

# --- 웹훅 알림 헬퍼 함수 정의 끝 ---

# --- 재난문자 감지 스케줄러 함수 시작 ---
# 1분마다 실행되며, Event Hubs로 데이터를 보내고 Teams에 알림을 보냄
@app.timer_trigger(schedule="0 */5 * * * *", arg_name="timer", run_on_startup=True, use_monitor=False)
@app.event_hub_output(arg_name="event", event_hub_name=os.environ["EventHubName"], connection="EventHubConnectionString")
def disaster_message_scheduler(timer: func.TimerRequest, event: func.Out[str]) -> None:
    logging.info('재난문자 감지 스케줄러 시작되었습니다.')

    # --- 1. 환경 변수 및 초기 변수 설정 ---
    container_name = "1dt022-blob" 
    blob_name = "last_processed_disaster_msg_id.txt" 
    last_processed_msg_id = "" # Blob에서 로드할 이전 재난문자 ID
    
    # 환경 변수에서 Blob Storage 연결 문자열과 재난문자 API 키를 가져옴
    blob_connection_string = os.environ.get("BlobStorageConnectionString")
    api_key = os.environ.get("DisasterMsgApiKey")
    api_url = "https://www.safetydata.go.kr/V2/api/DSSP-IF-00247" 
    today_date = datetime.datetime.now().strftime("%Y%m%d") 

    # 재난문자 API 요청 파라미터 
    api_params = {
        "serviceKey": api_key,  
        "pageNo": "1",          
        "numOfRows": "10",      
        "returnType": "json",   
        "crtDt": today_date     
    }

    latest_messages_from_api = [] # API로부터 최종적으로 수집된 메시지 목록

    # --- 2. Blob Storage에서 이전 처리 ID 로드 ---
    # 오류 발생 시 Teams에 알림을 보내고 함수를 종료
    try:
        if not blob_connection_string:
            logging.error("BlobStorageConnectionString이 환경 변수에 설정되지 않았습니다.")
            send_teams_webhook("오류: Blob Storage 연결 문자열이 없습니다! (재난문자 스케줄러)")
            return 

        blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)

        if not container_client.exists():
            container_client.create_container()
            logging.info(f"Blob 컨테이너 '{container_name}'이 생성되었습니다.")

        if blob_client.exists():
            last_processed_msg_id = blob_client.download_blob().readall().decode('utf-8').strip()
            logging.info(f"Blob에서 이전 재난문자 ID 로드 성공: '{last_processed_msg_id}'")
        else:
            logging.info(f"Blob '{blob_name}'을 찾을 수 없습니다. 새로운 상태 파일 생성 예정.")

    except Exception as e:
        logging.error(f"Blob Storage에서 이전 재난문자 ID 로드 중 치명적인 오류 발생: {e}")
        send_teams_webhook(f" 오류: Blob Storage 상태 로드 실패! ({e}) (재난문자 스케줄러)")
        last_processed_msg_id = "" # 오류 발생 시, 새 메시지로 간주하기 위해 ID 초기화
        
    # --- 3. 재난문자 API 호출 및 응답 처리 ---
    # 오류 발생 시 Teams에 알림을 보내고 함수를 종료
    try:
        if not api_params.get("serviceKey"): # API 키가 유효한지 최종 확인
            logging.error("API 키(serviceKey)가 api_params에 정의되지 않았습니다.")
            send_teams_webhook(" 오류: 재난문자 API 키가 설정되지 않았습니다!")
            return

        logging.info(f"재난문자 API 호출 시도: {api_url}")
        response = requests.get(api_url, params=api_params, timeout=10)
        response.raise_for_status() # HTTP 오류 검사 
        data = response.json()

        # API 응답에서 메시지 목록 추출
        if "body" in data and isinstance(data["body"], list):
            messages_from_api = data["body"]
            # 메시지를 'CRT_DT' (생성일시)와 'SN' (고유번호) 기준으로 최신순 정렬
            latest_messages_from_api = sorted(messages_from_api, 
                                             key=lambda x: (x.get('CRT_DT', ''), x.get('SN', 0)), 
                                             reverse=True)
            logging.info(f"API로부터 {len(latest_messages_from_api)}개의 재난문자 메시지 수집 성공.")
        else:
            logging.warning("재난문자 API 응답에서 'body' 데이터를 찾을 수 없습니다.")
            send_teams_webhook(f"경고: 재난문자 API 응답에 데이터가 없습니다. (재난문자 스케줄러)")

    except requests.exceptions.RequestException as e:
        logging.error(f"재난문자 API 요청 중 오류 발생: {e}")
        send_teams_webhook(f"오류: 재난문자 API 요청 실패! ({e}) (재난문자 스케줄러)")
        return
    except json.JSONDecodeError as e:
        logging.error(f"재난문자 API 응답 JSON 파싱 중 오류 발생: {e}")
        send_teams_webhook(f"🚨 오류: 재난문자 API 응답 파싱 실패! ({e}) (재난문자 스케줄러)")
        return
    except Exception as e:
        logging.error(f"재난문자 API 데이터 처리 중 예상치 못한 오류 발생: {e}")
        send_teams_webhook(f"🚨🚨 오류: 재난문자 데이터 처리 중 예상치 못한 오류! ({e}) (재난문자 스케줄러)")
        return
        
    # --- 4. 새로운 메시지 필터링 및 처리 ---
    new_messages_found = [] 
    current_most_recent_id = last_processed_msg_id # 현재 실행에서 가장 최근에 처리할 ID

    for msg in latest_messages_from_api:
        current_msg_id = msg.get('SN') 
        
        # ID를 정수로 변환 (비교를 위해)
        try:
            current_msg_id_int = int(current_msg_id) if current_msg_id is not None else 0
            last_processed_msg_id_int = int(last_processed_msg_id) if last_processed_msg_id else 0
        except ValueError:
            logging.warning(f"메시지 ID '{current_msg_id}' 또는 이전 ID '{last_processed_msg_id}'를 숫자로 변환 실패. 이 메시지는 건너뜀.")
            continue # 숫자로 변환할 수 없으면 이 메시지는 건너뜀

        if current_msg_id_int > last_processed_msg_id_int:
            new_messages_found.append(msg)
            logging.info(f"새로운 재난문자 감지: ID={current_msg_id}")
            # 이 실행에서 처리할 가장 최신 ID를 업데이트
            if current_msg_id_int > (int(current_most_recent_id) if current_most_recent_id else 0):
                current_most_recent_id = str(current_msg_id_int)
        else:
            logging.info(f"이전 처리된 재난문자 (ID: {current_msg_id}) - 건너뜀.")
            break # latest_messages_from_api가 최신순 정렬이므로, 이전 메시지 발견 시 종료


    # --- 5. 새로운 메시지 전송 (Event Hubs, Teams) 및 상태 저장 (Blob Storage) ---
    if new_messages_found:
        logging.info(f"총 {len(new_messages_found)}개의 새로운 재난문자 메시지 발견. 전송 시작.")
        # 새로운 메시지 중 가장 최신 메시지의 ID를 가져와 Blob Storage에 저장
        # 'new_messages_found' 리스트는 최신순으로 정렬된 메시지이므로, 첫 번째 요소가 가장 최신
        most_recent_msg_id_to_save = new_messages_found[0].get('SN') 
        
        # Event Hubs로 새로운 메시지 전송
        try:
            # 각 메시지 딕셔너리를 JSON 문자열로 변환 후 리스트로 Event Hubs에 보냄
            event.set([json.dumps(msg, ensure_ascii=False) for msg in new_messages_found])
            logging.info(f"Event Hubs로 {len(new_messages_found)}개의 새로운 재난문자 데이터 전송 완료.")
        except Exception as e:
            logging.error(f"Event Hubs로 재난문자 데이터 전송 실패: {e}")
            send_teams_webhook(f"오류: Event Hubs 전송 실패! ({e}) (재난문자 스케줄러)")

        # Teams로 새로운 메시지 알림
        for msg in new_messages_found:
            msg_id = msg.get('SN', 'N/A')
            msg_content = msg.get('MSG_CN', '내용 없음')
            create_date = msg.get('CRT_DT', 'N/A')
            rcptn_rgn_nm = msg.get('RCPTN_RGN_NM', '없음')
            emrg_step_nm = msg.get('EMRG_STEP_NM', '없음')
            dst_se_nm = msg.get('DST_SE_NM', '없음')

            teams_alert_message = (
                f"**새로운 재난문자 발생!** 🚨\n\n"
                f"**발송 일시:** {create_date}\n"
                f"**재난 유형:** {dst_se_nm} ({emrg_step_nm})\n"
                f"**수신 지역:** {rcptn_rgn_nm}\n"
                f"**내용:** {msg_content}\n\n"
                f"_재난문자 ID: {msg_id}_"
            )
            send_teams_webhook(teams_alert_message)
            logging.info(f"Teams에 재난문자 알림 전송 완료: ID {msg_id}")

        # Blob Storage에 마지막 처리 ID 저장
        try:
            if most_recent_msg_id_to_save is not None and most_recent_msg_id_to_save != "":
                blob_client.upload_blob(str(most_recent_msg_id_to_save), overwrite=True)
                logging.info(f"Blob '{blob_name}'에 최신 재난문자 ID '{most_recent_msg_id_to_save}' 저장 완료.")
            else:
                logging.warning("저장할 최신 재난문자 ID(SN)가 유효하지 않습니다. Blob 저장 건너뜀.")
                send_teams_webhook(f"경고: 최신 재난문자 ID를 가져오지 못하여 Blob 저장 실패. (재난문자 스케줄러)")
        except Exception as e:
            logging.error(f"Blob Storage에 최신 재난문자 ID 저장 실패: {e}")
            send_teams_webhook(f"오류: Blob Storage 상태 저장 실패! ({e}) (재난문자 스케줄러)")
            
    else:
        logging.info("새로운 재난문자 메시지가 없습니다. (변화 없음)")
        send_teams_webhook("재난문자 모니터링: 새로운 메시지 없음.")

    logging.info('재난문자 감지 스케줄러 종료되었습니다.')

# --- 재난문자 감지 스케줄러 함수 끝 ---