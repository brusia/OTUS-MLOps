terraform_init_validate_plan:
	cd infra && \
	terraform init -upgrade && \
	terraform validate && \
	terraform plan

create_storage: terraform_init_validate_plan
	cd infra && \
	terraform apply -auto-approve -target=module.storage

create_hadoop: terraform_init_validate_plan
	cd infra && \
	terraform apply -auto-approve

destroy:
	echo "Destroy dataproc cluster from infrastructure" && \
	cd infra && terraform destroy -target=module.dataproc -auto-approve && \
	echo "Remove backup file (in case you need to update startup-scripts it's nessesary to apply changes)" && \
	rm terraform.tfstate.backup && \
	echo "Object storage should not be destroyed (!). The data is still available on once created target bucket."

# use for debug
restart_proxy:
	cd infra && \
	terraform destroy -auto-approve -target=module.dataproc.yandex_compute_instance.proxy && \
	terraform init -upgrade && \
	terraform validate && \
	terraform plan && \
	terraform apply -auto-approve

compute: create_hadoop
	cd infra && terraform destroy -target=module.dataproc -auto-approve
