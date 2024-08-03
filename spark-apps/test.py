from datetime import datetime, timedelta

# Original scaled timestamp
scaled_timestamp = 15663.85588

# Convert scaled timestamp back to original timestamp
original_timestamp = scaled_timestamp * 1e5

# Convert original timestamp to datetime
date_time = datetime.fromtimestamp(original_timestamp)
print("With scaling:", date_time)



# Original scaled timestamp
scaled_timestamp = 15663.85588

# Calculate the datetime without scaling
date_time_without_scaling = datetime(1970, 1, 1) + timedelta(seconds=scaled_timestamp)
print("Without scaling:", date_time_without_scaling)