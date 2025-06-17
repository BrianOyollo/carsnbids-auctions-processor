#!/bin/bash

SESSION_NAME="airflow"
VENV_PATH="../.venv"

# Activate virtual environment command
ACTIVATE_CMD="source $VENV_PATH/bin/activate"

# Create a new tmux session detached
tmux new-session -d -s $SESSION_NAME

# Start API server
tmux new-window -t $SESSION_NAME -n api-server "bash -c '$ACTIVATE_CMD && airflow api-server --port 8080'"

# Start scheduler
tmux new-window -t $SESSION_NAME -n scheduler "bash -c '$ACTIVATE_CMD && airflow scheduler'"

# Start dag-processor
tmux new-window -t $SESSION_NAME -n dag_processor "bash -c '$ACTIVATE_CMD && airflow dag-processor'"

# Start triggerer
tmux new-window -t $SESSION_NAME -n triggerer "bash -c '$ACTIVATE_CMD && airflow triggerer'"

echo "All Airflow components started in tmux session: $SESSION_NAME"
echo "Attach using: tmux attach -t $SESSION_NAME"
