#!/bin/bash

echo "Stopping all Airflow services..."

# Send Ctrl+C to all panes in the tmux session
tmux send-keys -t airflow C-c

# Kill tmux session if still running
tmux kill-session -t airflow

echo "All Airflow services stopped."
