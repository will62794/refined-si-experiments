# PUT,548982,k1 -> 100007000
# PUT,548982,k129225 -> 100007001
# PUT,548991,k1 -> 100007000
# PUT,548991,k129225 -> 100007001
# PUT,549000,k1 -> 100007000
# PUT,549000,k129225 -> 100007001
# PUT,549009,k13877 -> 99969000
# PUT,549009,k0 -> 99969001
# PUT,549011,k43 -> 100005000
# PUT,549011,k21541 -> 100005001
# PUT,549011,k9054 -> 100005002
# PUT,549011,k3 -> 100005003
# PUT,549011,k183328 -> 100005004
# PUT,549011,k353 -> 100005005
# PUT,549017,k0 -> 99949000
# PUT,549017,k750 -> 99949001
# PUT,549017,k0 -> 99949002
#
# PUT,seqno,key -> value

import json

# Assuming a file bmrocks.log with the above format of events, one per line
# process it by extracting each event and storing a map from each value to its version (i.e. seqno)
def process_bmrocks_log(log_file):
    vmap = {}
    with open(log_file, 'r') as f:
        for line in f:
            if not line.startswith("PUT"):
                continue
            event = line.strip()
            if event:
                parts = event.split(',')
                # print(parts)
                if len(parts) == 4:
                    evtype = parts[0]
                    seqno = parts[1]
                    key = parts[2]
                    value = int(parts[3])
                    vmap[value] = int(seqno)
    return vmap

if __name__ == "__main__":
    version_map = process_bmrocks_log("bmrocks.log")
    # for v in version_map:
    #     print(v, version_map[v])
    print(f"Number of values in version map: {len(version_map)}")

    # load event_log.json and, for each each event, 
    # go through each write and check the value and looks up
    # its version in the version map, and assign this as the "version"
    # field value to that event.
    with open("event_log.json", "r") as f:
        events = json.load(f)
    num_vals_not_found = 0
    for event in events:
        if event["type"] == "ok":
            for write in event["value"]:
                if write[0] == "w":
                    value = int(write[2])
                    if value not in version_map:
                        # print(f"Value {value} missing from version map")
                        num_vals_not_found += 1
                        continue
                    version = int(version_map[value])
                    event["version"] = version

    if num_vals_not_found > 0:
        print(f"WARNING: Number of values missing from version map: {num_vals_not_found}, total events: {len(events)}")
    else:
        print(f"Number of values missing from version map: {num_vals_not_found}, total events: {len(events)}")
    with open("event_log_with_versions.json", "w") as f:
        json.dump(events, f, indent=2)