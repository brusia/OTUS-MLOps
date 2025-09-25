
mkdir feature-store
python3 -m uv add feast
python3 -m uv add 

python3 -m uv sync
git clone ${git_repo}
cd ${feature_repo_path}
source .venv/bin/python
feast apply
feast ui

# deactivate
# python3 -m uv run test_workflow.py > result.txt