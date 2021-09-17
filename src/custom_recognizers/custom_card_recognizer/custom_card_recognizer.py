from presidio_analyzer import PatternRecognizer, Pattern

def get_recognizer():
    # Define the regex pattern in a Presidio `Pattern` object:
    pattern = Pattern(name="card_pattern",regex="\d{15}", score =1)

    # Define the recognizer with one or more patterns
    return PatternRecognizer(supported_entity="CUSTOM_CREDIT_CARD", patterns = [pattern])