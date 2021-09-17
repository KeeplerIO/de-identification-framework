from presidio_analyzer import PatternRecognizer, Pattern

def get_recognizer():
    # Define the regex pattern in a Presidio `Pattern` object:
    user_id_pattern = Pattern(name="user_id_pattern",regex="user-.+", score =1)

    # Define the recognizer with one or more patterns
    return PatternRecognizer(supported_entity="USER_ID", patterns = [user_id_pattern])