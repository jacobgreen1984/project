# ---
# title   : 216_NL_PL_BL_AML
# version : 1.0
# author  : 2e consulting
# desc.   : 보험계약대출예측모델 Auto ML 튜닝
# ---


ML_AML <- h2o.automl(
  training_frame = train,
  validation_frame = valid,
  x = x,
  y = y,
  seed = 1234,
  
  # stoping options
  # max_runtime_secs = max_runtime_secs,
  max_models = max_models_AML
)

# print Log
print(ML_AML@leaderboard)
cat("AUTO ML done!","\n")
gc(reset=T)


