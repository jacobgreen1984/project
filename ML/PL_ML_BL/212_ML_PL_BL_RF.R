# ---
# title   : 212_NL_PL_BL_RF
# version : 1.0
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 Random Forest 튜닝
# ---


# -------------------------------------------------------------------------------
# cartesian grid search
# -------------------------------------------------------------------------------
h2o.rm("Cartesian_Grid_RF")

grid <- h2o.grid(
  hyper_params = list(max_depth = c(20,30,40,50,60,70)),
  search_criteria = list(strategy = "Cartesian"),
	algorithm="drf",
	grid_id="Cartesian_grid_RF",
	training_frame = train,
	validation_frame = valid,
	x = x,
	y = y,
	seed = 1234,

	# stoping options
	ntrees = 1000,
	stopping_rounds = 3,
	score_tree_interval = 6,
	stopping_tolerance = 1e-4,
	stopping_metric = "AUC"
	)

	# sort grid
	sortedGrid <- h2o.getGrid("Cartesian_Grid_RFM",sort_by = "AUC",decreasing = T)
	topDepths  <- sortedGrid@summary_table$max_depth[1:3]
	print(sortedGrid)

	# print grid
	minDepth <- min(as.numeric(topDepths))
	maxDepth <- max(as.numeric(topDepths))
	cat("minDepth: ",minDepth,"\n")
	cat("maxDepth: ",maxDepth,"\n")
# -------------------------------------------------------------------------------

	
# -------------------------------------------------------------------------------
# random grid search
# -------------------------------------------------------------------------------
h2o.rm("Random_Grid_RF")
	
hyper_params <- list( 
  max_depth = seq(minDepth,maxDepth,5),
  # mtries = unique(c(-1,seq(from=50,to=Length(x),by=50).Length(x))),
  sample_rate = seq(0.6,1,0.1),
  col_sample_rate_per_tree = seq(0.6,1,0.1),
  min_rows = c(1,2,5,10,20,50),
  nbins = 2^seq(4,10,1)
)

search_criteria <- list(
  strategy = "RandomDiscrete",
  max_runtime_secs = max_runtime_secs,
  max_models = max_models_RF,
  seed = 1234
)

grid <- h2o.grid(
  hyper_params = hyper_params,
  search_criteria = search_criteria,
  algorithm = "drf",
  grid_id = "Random_Grid_RF",
  training_frame = train,
  validation_frame = valid,
  x = x,
  y = y,
  seed = 1234,
  
  # stoping options 
  ntrees = 1000,
  stopping_rounds = 3,
  score_tree_interval = 6,
  stopping_tolerance = 1e-4,
  stopping_metric = "AUC"
)

# print grid
sortedGrid = h2o.getGrid("Random_Grid_RF",sort_by="AUC",decreasing=T)
print(sortedGrid)

# print Log
cat("RF done!","\n")
gc(reset=T)
# -------------------------------------------------------------------------------