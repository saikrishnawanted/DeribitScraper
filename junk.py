import main
import DeribitApi
import JoinAllFiles


def _update_and_write_tick_data(self, last_trades, prev_tickdata, outdir):
    """
    Merges previously scraped data with new data scraped today. Results in a file Data/<contract>/tickdata.csv
    :param last_trades:
    :param prev_tickdata:
    :param outdir:
    :return:
    """
    new_data = pd.DataFrame(last_trades)
    new_data = pd.concat([prev_tickdata, new_data], axis=0, sort=False)
    new_data['date_utc'] = pd.to_datetime(
        new_data['timeStamp'].apply(lambda x: datetime.utcfromtimestamp(x / 1000).strftime('%Y-%m-%dT%H:%M:%S.%f'))
    )
    new_data = new_data.drop_duplicates(subset='tradeId', keep='last')
    new_data = new_data.sort_values('date_utc')
    new_data = new_data.set_index('date_utc')
    new_data.to_csv(outdir + "/tickdata.csv", index=True)
    return new_data

_update_and_write_tick_data()