SELECT DATE_CODE, PREDICTION, CONFIDENCE, ACTUAL, MODEL_ID 
FROM {{ ref("transformed_prediction_data") }}
WHERE PREDICTION IS NOT NULL