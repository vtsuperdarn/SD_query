import pyarrow.parquet as pq
import pandas
import numpy
import datetime


class SDParquetUtils(object):
    """
    A class to read from parquet
    files containing fitacf data!
    """
    def __init__(self, fileList, readAllColumns=False,\
             readColList=["time", "bmnum", "v", "p_l", "w_l"]):
        """
        setup parameters
        (1): We can read from multiple files at once
        and that's why we use the fileList keyword!
        (2) Most of the time we only need a few columns
        from the data file (for ex vel, time etc). So we'll
        only read those. But if you need all the columns,
        just set readAllColumns to True. In this case readColList
        will be ignored
        """
        self.fileList = fileList
        self.readAllColumns = readAllColumns
        self.readColList = readColList

    def get_data(self, toPandas=True, flattenDF=True, flattenCols=[],**kwargs):
        """
        get the data from the files
        we output the results as either
        a pandas DF or a dict!
        **kwargs are pretty much the same as dataset.read
        these include nthreads and other similar one's.
        """
        # we'll use the dataset api to read the data
        dataset = pq.ParquetDataset(self.fileList)
        if not self.readAllColumns:
            table = dataset.read( columns=self.readColList, **kwargs )
        else:
            table = dataset.read( **kwargs )
        # It is a simple option to return a dict
        if not toPandas:
            return table.to_pydict()
        # Return a pandas dataframe!
        # not too bad either if you don't want a flattened DF
        # i.e., in the dataframe columns such as vel, power
        # and other similar one's will be lists! which means they
        # are not very analysis friendly!
        if not flattenDF:
            return table.to_pandas()
        else:
            # Now flatten the DF. It is difficult to guess which cols
            # are lists, we'll expect the users to give the info!
            return "yet to do!"




if __name__ == "__main__":
    # create a list of files to loop through
    inpDir = "/sd-data/parquet/2012/fhe/"
    startDate = datetime.datetime(2012,6,1)
    endDate = datetime.datetime(2012,6,3)
    fList = []
    while startDate <= endDate:
        fList.append( inpDir + startDate.strftime("%Y%m%d") + "fhe.parquet" )
        startDate += datetime.timedelta(days=1)    
    spO = SDParquetUtils(fList)
    df = spO.get_data(flattenDF=False)
    print df.head()
    print df.shape
