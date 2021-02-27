#!/bin/bash
export PROJECT=your-project
export BUCKET=your-bucket
export REGION=us-central1
export RUNNER=DataflowRunner
#export RUNNER=DirectRunner
#export MODE=COST_OPTIMIZED
export MODE=SPEED_OPTIMIZED
export INPUT=input-your-subscription
#export MACHINE_TYPE=n1-highmem-16
export MACHINE_TYPE=n1-standard-2
python3 strem_count.py \
	--region $REGION \
	--input_subscription  $INPUT \
	--runner $RUNNER \
	--project $PROJECT \
	--flexrs_goal $MODE \
	--machine_type  $MACHINE_TYPE \
	--temp_location gs://$BUCKET/tw_sample_tmp/ 



