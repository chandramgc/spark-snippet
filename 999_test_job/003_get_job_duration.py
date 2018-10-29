import datetime, time
startTime = datetime.datetime.now()
time.sleep(2)
stopTime = datetime.datetime.now()
elapsedTime = stopTime - startTime
duration_in_s = elapsedTime.total_seconds()
print "Seconds " + str(duration_in_s)                         # Seconds
print "Minutes " + str(divmod(duration_in_s, 60)[0])          # Seconds in a minute = 60
print "Hours   " + str(divmod(duration_in_s, 3600)[0])        # Seconds in an hour = 3600
print "Days    " + str(divmod(duration_in_s, 86400)[0])       # Seconds in a day = 86400
print "Years   " + str(divmod(duration_in_s, 31556926)[0])    # Seconds in a year=31556926


