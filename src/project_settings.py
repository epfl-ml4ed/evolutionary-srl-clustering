# gridsearch of features
PREFIX = 'full_gs'
#current experiment
TEST_ID = 'jun6'


FEATURE_GROUPS =  {

'effort': {'metric': 'dtw',
           'features': ['total_duration',
                        'writing_events']},

'consistency': {'metric': 'dtw',
           'features': ['average_duration_session',
                        'total_duration_norm',
                        'writing_events_norm']},

'quality': {'metric': 'dtw',
            'features': ['image_ratio',
                        'average_reflection',
                          'tags_ratio',
                          'ingredients_ratio']},

'regularity': {'metric': 'euclidean',
               'features': ['pwd_biweek', 'fdh','fwh']},

'feedback': {'metric': 'dtw',
             'features': ['request_feedback_ratio',
                        'response_request_ratio']}
    }



FEATURE_GROUPS_GENEVA =  {

'effort': {'metric': 'dtw',
           'features': ['total_duration',
                        'writing_events']},

'consistency': {'metric': 'dtw',
           'features': ['average_duration_session',
                        'total_duration_norm',
                        'writing_events_norm']},

'quality': {'metric': 'dtw',
            'features': ['image_ratio',
                        'average_reflection',
                          'tags_ratio',
                          'ingredients_ratio']},

'regularity': {'metric': 'euclidean',
               'features': ['pwd', 'fdh', 'fwh']},

'feedback': {'metric': 'dtw',
             'features': ['request_feedback_ratio',
                        'response_request_ratio']}
    }
