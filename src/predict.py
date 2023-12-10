import pandas as pd
import pickle

def load_models(model_paths):
    """
    Load models from pickle files.

    Parameters:
    model_paths (List[str]): List of file paths for the pickle files.

    Returns:
    List[object]: List of loaded models.
    """
    loaded_models = []
    for model_path in model_paths:
        with open(model_path, 'rb') as file:
            loaded_models.append(pickle.load(file))
    return loaded_models

def make_predictions(models, data):
    """
    Make predictions using loaded models and input data.

    Parameters:
    models (List[object]): List of loaded models.
    data (pd.DataFrame): Input data for prediction.

    Returns:
    List[pd.Series]: List of prediction results.
    """
    predictions = [model.predict(data) for model in models]
    return predictions

def main():
    # Define file paths for pickle models and parquet data
    model_paths = ["neural_net_model.pkl", "adaboost_model.pkl"]  # Update with your model paths
    parquet_path = "input_data.parquet"  # Update with your parquet file path
    output_path = "predictions.csv"  # Update with your desired output path

    # Load models
    models = load_models(model_paths)

    # Load parquet data for prediction
    data = pd.read_parquet(parquet_path)

    # Make predictions
    predictions = make_predictions(models, data)

    # Create a DataFrame with predictions
    predictions_df = pd.DataFrame({"Model": [f"Model {i+1}" for i in range(len(models))], "Prediction": predictions})

    # Save predictions to a CSV file
    predictions_df.to_csv(output_path, index=False)
    print(f"Predictions saved to {output_path}")
    return predictions_df

