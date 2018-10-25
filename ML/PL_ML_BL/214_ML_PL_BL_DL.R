# ---
# title   : 214_NL_PL_BL_DL
# version : 1.0
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 Deep Learning 튜닝
# ---


# -------------------------------------------------------------------------------
# random grid search
# -------------------------------------------------------------------------------
h2o.rm("Random_Grid_DL")

hyper_params <- list( 
  hidden = list(c(100),c(200),c(300),c(100,100),c(200,200),c(300,300),c(100,100,100),c(200,200,200),c(300,300,300)),
  activation = c("Rectifier","Tanh","RectifierWithDropout","TanhWithDropout"),
  input_dropout_ratio = c(0,0.05,0.1,0.15,0.2,0.25,0.3,0.35,0.4),
  l1 = c(0,0.1,0.01,0.001,0.0001),
  l2 = c(0,0.1,0.01,0.001,0.0001)
)

search_criteria <- list(
  strategy = "RandomDiscrete",
  max_runtime_secs = max_runtime_secs,
  max_models = max_models_DL,
  seed = 1234
)

grid <- h2o.grid(
  hyper_params = hyper_params,
  search_criteria = search_criteria,
  algorithm = "deeplearning",
  grid_id = "Random_Grid_DL",
  training_frame = train,
  validation_frame = valid,
  x = x,
  y = y,
  seed = 1234,
  
  # overfitting options
  rate = 0.005,
  rate_annealing = 1e-06,
  standardize = T,
  
  # stoping options 
  epochs = 1000,
  stopping_rounds = 3,
  stopping_tolerance = 1e-4,
  stopping_metric = "AUC"
)

# print grid
sortedGrid = h2o.getGrid("Random_Grid_DL",sort_by="AUC",decreasing=T)
print(sortedGrid)

# print Log
cat("DL done!","\n")
gc(reset=T)
# -------------------------------------------------------------------------------