terraform_init_validate_plan:
	cd infra/main && \
	terraform init -upgrade && \
	terraform validate && \
	terraform plan

create_managing_infra: terraform_init_validate_plan
	cd infra/main && \
	terraform apply -auto-approve
#  -target=module.managing_proxy

# create_hadoop: terraform_init_validate_plan
# 	cd infra/main && \
# 	terraform apply -auto-approve

destroy:
	echo "Destroy dataproc cluster from infrastructure" && \
	cd infra && terraform destroy -target=module.managing_proxy -auto-approve && \
	echo "Remove backup file (in case you need to update startup-scripts it's nessesary to apply changes)" && \
	rm terraform.tfstate.backup && \
	echo "Object storage should not be destroyed (!). The data is still available on once created target bucket."

# use for debug
restart_proxy:
	cd infra/main && \
	terraform destroy -auto-approve -target=module.managing_proxy && \
	terraform init -upgrade && \
	terraform validate && \
	terraform plan && \
	terraform apply -auto-approve -target=module.managing_proxy

compute: create_manager
	cd infra/main && terraform destroy -target=module.managing_proxy -auto-approve

sync_dags:
	rsync -a dags/ ubuntu@ya_proxy:/home/ubuntu/dags/ && \
	ssh ubuntu@ya_proxy "sudo chown -R ubuntu:ubuntu /home/ubuntu/dags"