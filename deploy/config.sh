project=kernelorg-mirror
region=us-central1
name=lore-mirror
build_sa="lore-build@$project.iam.gserviceaccount.com"
source_bucket="gs://$project-build-source"
infra_sa="lore-infra@$project.iam.gserviceaccount.com"
deployment="projects/$project/locations/$region/deployments/$name"

gcloud() { command gcloud --project="$project" --quiet "$@"; }

# Hold the checkout's infra directory lock until the deployment process exits.
lock_deployment() {
  exec 9<infra
  flock 9
}

apply_deployment() {
  gcloud infra-manager deployments apply "$deployment" \
    --service-account="projects/$project/serviceAccounts/$infra_sa" \
    --tf-version-constraint='=1.5.7' --local-source=infra
}
