echo '###--- START of deploy.sh ---###'
set -e
source "/workspace/part-4/scripts/.env.local"
echo "${DIR}"
CODE_PY_DIR="${DIR}/../src"
PROJECT_ID=$(gcloud config get-value project)


echo $CODE_PY_DIR
echo $PROJECT_ID

echo '--- Deployment of Cloud Run Function ---'
gcloud functions deploy ${FUNCTION_NAME} \
    --region=${REGION} \
    --source=${CODE_PY_DIR} \
    --entry-point=${ENTRY_POINT} \
    --gen2 \
    --trigger-http \
    --runtime=python312 \
    --min-instances=${MIN_INSTANCES} \
    --max-instances=${MAX_INSTANCES} \
    --memory=${FUNCTION_MEMORY} \
    --timeout=${FUNCTION_TIMEOUT} \
    --update-labels=developer=lordwin,gcp-service=cloud_function

echo '--- Update Cloud Run Revision ---'
gcloud run services update ${FUNCTION_NAME} --region=${REGION} --execution-environment=gen2


echo '--- Deployment of Cloud Scheduler ---'
gcloud scheduler jobs create http ${CLOUD_SCHEDULER_NAME} --location=${REGION} \
    ---schedule="${CS_SCHEDULER_RUN}" \
    --uri="https://${REGION}-${PROJECT_ID}.cloudfunctions.net/${FUNCTION_NAME}" \
    --http-method=${CS_HTTP_METHOD} \
    --description="${CS_DESC}" \
    --time-zone=${CS_TZ} \
    --headers=Content-Type="application/json",User-Agent="Google-Cloud-Scheduler"



        # --message-body='{"de_job_id":"'${DE_JOB_ID}'", "de_job_name":"'${FUNCTION_NAME}'"}' \
        # --oidc-service-account-email=${SERVICE_ACCOUNT} \
        # --attempt-deadline=${CS_ATTEMPT_DEADLINE} 
