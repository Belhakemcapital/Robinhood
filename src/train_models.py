import pickle
from sklearn.ensemble import AdaBoostClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.model_selection import train_test_split

def train_and_export_models(X, y, model_names, classifiers, pickle_paths):
    """
    Train machine learning models and export them as pickle files.

    Parameters:
    X (pd.DataFrame): Features for training.
    y (pd.Series): Target variable for training.
    model_names (List[str]): List of model names.
    classifiers (List[object]): List of classifier objects.
    pickle_paths (List[str]): List of file paths for saving pickle files.

    Returns:
    List[object]: List of trained classifiers.
    """
    trained_classifiers = []

    # Split the data into training and testing sets
    X_train, X_test, Y_train, Y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    for name, clf, pickle_path in zip(model_names, classifiers, pickle_paths):
        # Train the classifier
        clf.fit(X_train, Y_train)
        trained_classifiers.append(clf)

        # Export the trained model as a pickle file
        with open(pickle_path, 'wb') as file:
            pickle.dump(clf, file)
        
    return trained_classifiers

