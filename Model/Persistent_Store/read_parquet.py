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

    def get_data(self, toPandas=True, flattenDF=False, flattenCols=[],**kwargs):
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
            if flattenDF:
                if len(flattenCols) > 0.:
                    return self.get_flattened_df(table.to_pandas(), flattenCols)
                print("Need atleast one column to flatten")
                return None


    def get_flattened_df(self, parqDF, flattenCols):
        """
        Some of the cols in the original DF are list (for ex vels)
        so we'll flatten them for easier data manipulation!
        """
        def _split_list_to_rows(row,rowList,splitColList):
            splitRows = {}
            maxSplit = 0
            for splitCol in splitColList:
                split_row = row[splitCol]#.split(row_delimiter)
                splitRows[splitCol] = split_row
                if split_row is None:
                    continue
                if len(split_row) > maxSplit:
                    maxSplit = len(split_row)
                
            for i in range(maxSplit):
                nRow = row.to_dict()
                for splitCol in splitColList:
                    nRow[splitCol], splitRows[splitCol] =\
                                splitRows[splitCol][0], splitRows[splitCol][1:]
                rowList.append(nRow)

        newRows = []
        parqDF.apply(_split_list_to_rows,axis=1,args = (newRows,flattenCols))
        outDF = pandas.DataFrame(newRows, columns=parqDF.columns)
        return outDF


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
    df = spO.get_data(flattenDF=True, flattenCols=["v", "w_l", "p_l"])
    print( df.head() )
    print( df.shape )
