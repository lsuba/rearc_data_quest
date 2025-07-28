echo '###--- START of deploy.sh ---###'
set -e
source "/workspace/part-4/scripts/.env.local"
echo "${DIR}"
CODE_PY_DIR="${DIR}/src"
PROJECT_ID=$(gcloud config get-value project)
SECRET_PROJECT=$(gcloud projects list --filter="$(gcloud config get-value project)" --format="value(PROJECT_NUMBER)")
TRIGGER_CODE_PY_DIR="${DIR}/src_2"


echo $CODE_PY_DIR
echo $TRIGGER_CODE_PY_DIR
echo $PROJECT_ID
SERVICE_ACCOUNT="${SECRET_PROJECT}-${CS_SA}"
echo $SERVICE_ACCOUNT

# echo '#--- Deployment of Cloud Run Function ---#'
# gcloud functions deploy ${FUNCTION_NAME} \
#     --region=${REGION} \
#     --source=${CODE_PY_DIR} \
#     --entry-point=${ENTRY_POINT} \
#     --gen2 \
#     --trigger-http \
#     --runtime=python312 \
#     --min-instances=${MIN_INSTANCES} \
#     --max-instances=${MAX_INSTANCES} \
#     --memory=${FUNCTION_MEMORY} \
#     --timeout=${FUNCTION_TIMEOUT} \
#     --cpu=${CPU_LIMIT} \
#     --ingress-settings=all \
#     --no-allow-unauthenticated \
#     --update-labels=developer=lordwin,gcp-service=cloud_function

# echo '#--- Update Cloud Run Revision ---#'
# gcloud run services update ${FUNCTION_NAME} --region=${REGION} --execution-environment=gen2


# echo '#--- Deployment of Cloud Scheduler ---#'
# gcloud scheduler jobs create http ${CLOUD_SCHEDULER_NAME} --location=${REGION} \
#     --schedule="${CS_SCHEDULER_RUN}" \
#     --uri="https://${REGION}-${PROJECT_ID}.cloudfunctions.net/${FUNCTION_NAME}" \
#     --http-method=${CS_HTTP_METHOD} \
#     --description="${CS_DESC}" \
#     --time-zone=${CS_TZ} \
#     --headers=Content-Type="application/json",User-Agent="Google-Cloud-Scheduler" \
#     --attempt-deadline=${CS_ATTEMPT_DEADLINE} \
#     --oidc-service-account-email=${SERVICE_ACCOUNT} \
#     --message-body='{"de_job_id":"'${DE_JOB_ID}'", "de_job_name":"'${DE_JOB_NAME}'", "project_name":"'${PROJECT_ID}'"}'


echo '#--- Deployment of Trigger CRF via GCS Event ---#'
gcloud functions deploy ${TRIGGER_FUNCTION_NAME} \
    --region=${REGION} \
    --source=${TRIGGER_CODE_PY_DIR} \
    --entry-point=${TRIGGER_ENTRY_POINT} \
    --gen2 \
    --trigger-event=${EVENT_TYPE} \
    --trigger-resource=${EVENT_RESOURCE} \
    --trigger-location=us \
    --runtime=python312 \
    --min-instances=${MIN_INSTANCES} \
    --max-instances=${MAX_INSTANCES} \
    --memory=${FUNCTION_MEMORY} \
    --timeout=${TRIGGER_FUNCTION_TIMEOUT} \
    --cpu=${CPU_LIMIT} \
    --ingress-settings=all \
    --no-allow-unauthenticated \
    --update-labels=developer=lordwin,gcp-service=cloud_function

# --trigger-resource=${BUCKET_NAME} \
    # 

# echo '#--- Update Trigger CRF via GCS Event ---#'
# gcloud run services update ${TRIGGER_FUNCTION_NAME} --region=${REGION} --execution-environment=gen2


echo '#--- Trigger Cloud Run Function ---#'
# gcloud functions call ${FUNCTION_NAME} --region=${REGION} \
#     --data='{"de_job_id":"'${DE_JOB_ID}'", "de_job_name":"'${DE_JOB_NAME}'"}'

gcloud functions call ${FUNCTION_NAME} --region=${REGION} \
    --data '{"de_job_id":"'${DE_JOB_ID}'", "de_job_name":"'${FUNCTION_NAME}'", "project_name":"'${PROJECT_ID}'"}'


        # --message-body='{"de_job_id":"'${DE_JOB_ID}'", "de_job_name":"'${FUNCTION_NAME}'"}' \
        # --oidc-service-account-email=${SERVICE_ACCOUNT} \
        # --attempt-deadline=${CS_ATTEMPT_DEADLINE} 