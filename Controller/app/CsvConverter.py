import pandas


def contert_to_json(time, packet_loss, bandwidth, delay):
    test_df = pandas.read_csv("RTT Raspberry.csv", usecols=[time, packet_loss, bandwidth, delay])

    test_df.to_json()
