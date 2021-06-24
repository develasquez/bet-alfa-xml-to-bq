

**Declare global vars**

```sh
export PROJECT_ID=
export SUBNETWORK=default	
export ORIGIN_GCS=gs://
export REGION=us-central1
export ZONE=us-central1-a
export OUTPUT=$PROJECT_ID:<dataset>.<table>
```

**Create bucket topic**
```sh
gsutil notification create -t readfile -f json $ORIGIN_GCS
```

**Run dataflow Job**

```sh
python -m readfile \
--runner DataflowRunner \
--project $PROJECT_ID \
--stagingLocation gs://$PROJECT_ID-df/readfile/staging \
--tempLocation gs://$PROJECT_ID-df/readfile/temp \
--region $REGION \
--zone $ZONE \
--output $OUTPUT \
--topic projects/$PROJECT_ID/topics/readfile \
--rawFilesLocation gs://$PROJECT_ID-df/readfile/backup \
--subnetwork=regions/$REGION/subnetworks/$SUBNETWORK \
--experiments=allow_non_updatable_job \
--requirements_file requirements.txt
```