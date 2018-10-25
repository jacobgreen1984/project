# ---
# title   : 213_NL_PL_BL_GBM
# version : 1.0
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 Gradient Boosting Machine 튜닝
# ---


# -------------------------------------------------------------------------------
# cartesian grid search
# -------------------------------------------------------------------------------
h2o.rm("Cartesian_Grid_GBM")

grid <- h2o.grid(
  hyper_params = list(max_depth = c(3,5,7,9,11,13,15,17,19)),
  search_criteria = list(strategy = "Cartesian"),
  algorithm="gbm",
  grid_id="Cartesian_Grid_GBM",
  training_frame = train,
  validation_frame = valid,
  x = x,
  y = y,
  seed = 1234,
  
  # overfitting options
  learn_rate = 0.1,
  categorical_encoding = "SortByResponse",
  
  # stoping options
  ntrees = 1000,
  stopping_rounds = 3,
  score_tree_interval = 6,
  stopping_tolerance = 1e-4,
  stopping_metric = "AUC"
)

# sort grid
sortedGrid <- h2o.getGrid("Cartesian_Grid_GBM",sort_by = "AUC",decreasing = T)
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
h2o.rm("Random_Grid_GBM")

hyper_params <- list( 
  max_depth = seq(minDepth,maxDepth,1),
  sample_rate = seq(0.8,0.9,1),
  col_sample_rate = seq(0.6,1,0.1),
  col_sample_rate_per_tree = seq(0.6,1,0.1),
  min_rows = c(1,2,5,10,20,50),
  max_abs_leafnode_pred = c(Inf,1,2,3,4,5),
  learn_rate = c(0.1,0.05,0.01)
)

search_criteria <- list(
  strategy = "RandomDiscrete",
  max_runtime_secs = max_runtime_secs,
  max_models = max_models_GBM,
  seed = 1234
)

grid <- h2o.grid(
  hyper_params = hyper_params,
  search_criteria = search_criteria,
  algorithm = "gbm",
  grid_id = "Random_Grid_GBM",
  training_frame = train,
  validation_frame = valid,
  x = x,
  y = y,
  seed = 1234,
  
  # overfitting options
  categorical_encoding = "SortByResponse",
  
  # stoping options 
  ntrees = 1000,
  stopping_rounds = 3,
  score_tree_interval = 6,
  stopping_tolerance = 1e-4,
  stopping_metric = "AUC"
)

# print grid
sortedGrid = h2o.getGrid("Random_Grid_GBM",sort_by="AUC",decreasing=T)
print(sortedGrid)

# print Log
cat("GBM done!","\n")
gc(reset=T)
# -------------------------------------------------------------------------------