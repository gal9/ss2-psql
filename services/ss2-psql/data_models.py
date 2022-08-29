import json

alert_template = {
    "dateIssued": {
        "type": "Property",
        # eg. "value": "2017-01-02T09:25:55.00Z"
    },
    "description": {
        "type": "Property",
        "value": "Final leakage position detected" # title + Content
    },
    #"type": "Alert",
    # Attributes that get updated 
    "updatedAttributes": {
        "type": "Property", 
        "value": "dateIssued,description,ksiSignature"
    } 
}