bind = '[::]:8000'
proc_name = 'cpq-pricing-engine'
accesslog = 'gunicorn.log'
errorlog = 'gunicorn.error.log'
capture_output = True
timeout = 600
