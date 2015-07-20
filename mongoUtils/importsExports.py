"""Classes used to import/export data to mongoDB
"""

from Hellas.Thebes import format_header

xlrd = None  # reserved to import xlrd on demand


def _xlrd_on_demand():
    global xlrd
    if xlrd is None:
        try:
            import xlrd
        except ImportError:
            print ("this module requires xlrd library please install (pip install xlrd")
            raise
    return xlrd


def import_workbook(workbook, db, fields=None, ws_options={'dt_python': True}, stats_every=1000):
    """save all workbook's sheets to a db
    consider using :class:`~ImportXls` class instead which is more flexible but imports only a single sheet

    :Parameters: see :class:`~ImportXls` class

    :Example:
        >>> from pymongo import MongoClient
        >>> from mongoUtils import _PATH_TO_DATA
        >>> db = MongoClient().test
        >>> res = import_workbook(_PATH_TO_DATA + "example_workbook.xlsx", db)
        >>> res
        [{'rows': 368, 'db': 'test', 'collection': 'weather'}, {'rows': 1007, 'db': 'test', 'collection': 'locations'}]
    """
    _xlrd_on_demand()
    workbook = xlrd.open_workbook(workbook, on_demand=True)
    return [ImportXls(workbook, i, db, fields=fields, ws_options=ws_options, stats_every=stats_every)()
            for i in range(0,  workbook.nsheets)]


class Import(object):
    """generic class for importing into a mongoDB collection, successors should use/extend this class

    :Parameters:
        - db: a pynongo database object that will be used for output
        - collection: a pymongo collection object that will be used for output
        - drop_collection: (defaults to True)
            - True drops output collection on init before writing to it
            - False appends to output collection
        - stats_every: int print import stats every stats_every rows or 0 to cancel stats (defaults to 10000)
    """
    format_stats = "|{db:16s}|{collection:16s}|{rows:15,d}|"
    format_stats_header = format_header(format_stats)

    def __init__(self, collection, drop_collection=True, stats_every=10000):
        if drop_collection:
            collection.database.drop_collection(collection.name)
        self.info = {'db': collection.database.name, 'collection': collection.name, 'rows': 0}
        self.stats_every = stats_every
        self.collection = collection

    def import_to_collection(self):
        """successors should implement this"""
        raise NotImplementedError

    def _import_to_collection_before(self):
        """successors can call this or implement their's"""
        if self.stats_every > 0:
            print(self.format_stats_header)

    def _import_to_collection_after(self):
        """successors can call this or implement their's"""
        if self.stats_every > 0:
            self.print_stats()

    def print_stats(self):
        print(self.format_stats.format(**self.info))

    def __call__(self):
        return self.import_to_collection()


class ImportXls(Import):
    """save an an xls sheet to a collection
    `see <https://github.com/python-excel/xlrd>`_

    :Parameters:
        - workbook: path to a workbook or an xlrd workbook object
        - sheet: name of a work sheet in workbook or an int (sheet number in workbook)
        - db: a pymongo database object
        - coll_name: str output collection name or None to create name from sheet name (defaults to None)
        - row_start: int or None starting raw or None to start from first row (defaults to None)
        - row_end:int or None ending raw or None to end at lastrow (defaults to None)
        - fields:
            - a list with field names
            - or True (to treat first row as field names)
            - or None (for auto creating field names i.e: [fld_1, fld_2, etc]
            - or a function that:
                - takes one argument (a list of row values)
                - returns a dict  (if this dict contains a key '_id' this value will be used for _id)
                - >>> lambda x: {'coordinates': [x[0] , x[1]]}
        - ws_options: (optional) a dictionary specifying how to treat cell values
            - dt_python : bool convert dates to python datetime
            - integers_only : round float values to int helpful coz all int values are represented as floats in sheets
            - negatives_to_0 : treat all negative numbers as 0's
        - drop_collection: (defaults to True)
            - True drops output collection on init before writing to it
            - False appends to output collection
        - stats_every: int print import stats every stats_every rows or 0 to cancel stats (defaults to 10000)
        - drop_collection: if True drops collection on init otherwise appends to collection

    :Example:
        >>> from pymongo import MongoClient
        >>> from mongoUtils import _PATH_TO_DATA
        >>> db = MongoClient().test
        >>> res = ImportXls(_PATH_TO_DATA + "example_workbook.xlsx", 0, db)()
        >>> res
        {'rows': 367, 'db': u'test', 'collection': u'weather'}

    """

    def __init__(self,
                 workbook, sheet,
                 db, coll_name=None,
                 row_start=None, row_end=None,
                 fields=True,
                 ws_options={'dt_python': True, 'integers_only': False, 'negatives_to_0': False},
                 stats_every=10000,
                 drop_collection=True):
        _xlrd_on_demand()
        if not isinstance(workbook, xlrd.book.Book):
            workbook = xlrd.open_workbook(workbook, on_demand=True)
        self.workbook = workbook
        self.sheet = workbook.sheet_by_index(sheet) if isinstance(sheet, int) else workbook.sheet_by_name(sheet)
        self._ws_options = {}
        self.ws_options_set(ws_options)
        coll_name = self.fix_name(self.sheet.name) if coll_name is None else coll_name
        if row_start is None:
            row_start = 1 if fields is True else 0
        self.row_start = row_start
        self.row_end = row_end
        collection = db[coll_name]
        super(ImportXls, self).__init__(collection, drop_collection=drop_collection, stats_every=stats_every)
        self.auto_field_names(fields)

    @property
    def ws_options(self):
        return self._ws_options

    def ws_options_set(self, options_dict):
        self._ws_options.update(options_dict)

    def fix_name(self, name, cnt=0):
        if name == '':
            return 'fld_{}'.format(cnt)
        else:
            return name.replace(' ', '_').replace('.', '_').replace('$', '_')

    def auto_field_names(self, fields):
        row0_values = self.sheet.row_values(0)
        if fields is True:
            self._fields_or_fun = [self.fix_name(fn, cnt) for cnt, fn in enumerate(row0_values)]
        elif fields is None:
            self._fields_or_fun = ['fld_{}'.format(i) for i in range(len(row0_values))]
        elif isinstance(fields, list):
            self._fields_or_fun = [self.fix_name(fn, cnt) for cnt, fn in enumerate(fields)]
        else:  # then it has to be function
            self._fields_or_fun = fields
        return self._fields_or_fun

    def row_to_doc(self, valueslist, _id=None):
        if isinstance(self._fields_or_fun, list):
            doc = dict(list(zip(self._fields_or_fun, valueslist)))
        else:
            doc = self._fields_or_fun(valueslist)
        if _id is not None and doc.get('_id') is None:
            doc['_id'] = _id
        return doc

    def ws_convert_cell(self, cl):
        """
        :Parameters:
            - cl an xlrd cell object
        """
        #  XL_CELL_BLANK XL_CELL_BOOLEAN XL_CELL_NUMBER XL_CELL_TEXT
        tp = cl.ctype
        vl = cl.value
        if tp == xlrd.XL_CELL_NUMBER:   # number
            if self._ws_options.get('integers_only') is True:
                vl = int(vl + 0.49999)  # kind of round
            if vl < 0 and self._ws_options.get('negatives_to_0'):
                vl = 0
        elif tp == xlrd.XL_CELL_DATE and self._ws_options.get('dt_python') is True:
            vl = xlrd.xldate.xldate_as_datetime(vl, self.sheet.book.datemode)
        return vl

    def import_to_collection(self):
        super(ImportXls, self)._import_to_collection_before()
        outlist = []
        for i in range(self.row_start, self.row_end or self.sheet.nrows):
            self.info['rows'] += 1
            row_values = [self.ws_convert_cell(cl) for cl in self.sheet.row(i)]
            outlist.append(self.row_to_doc(row_values, i))
            if self.stats_every and i % self.stats_every == 0:
                self.print_stats()
            if len(outlist) == 200:
                try:
                    self.collection.insert_many(outlist)
                    outlist = []
                except Exception:
                    print (outlist)
                    raise
        if len(outlist) > 0:
            self.collection.insert_many(outlist)
        super(ImportXls, self)._import_to_collection_after()
        return self.info
