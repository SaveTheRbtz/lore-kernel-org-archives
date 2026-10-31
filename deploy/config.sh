project=kernelorg-mirror
region=us-central1
name=lore-mirror
build_sa="lore-build@$project.iam.gserviceaccount.com"
source_bucket="gs://$project-build-source"
infra_sa="lore-infra@$project.iam.gserviceaccount.com"
deployment="projects/$project/locations/$region/deployments/$name"

gcloud() { command gcloud --project="$project" --quiet "$@"; }
