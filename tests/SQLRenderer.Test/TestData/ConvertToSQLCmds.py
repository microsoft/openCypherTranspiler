import unicodedata, re
import sys
import operator
import csv
from datetime import datetime
from io import StringIO


# read file, determine if it is csv or tsv, then extract the data and automatically determine the best
# type for each field. Eventually output a .ss file that can be run with Cosmos run time to
# actually convert the csv/tsv file into .ss file


# parameters:
#  [1]: file to read
#  [2]: first row is header
#  [3]: script file to generate


# Contants

# supported delimiters and how to represent it in Scope code
delims = {
    # (Delimiter, C# representation)
    ',': ',',
    '\t': '\\t'
}

# supported type and represent it in Scope code
type_tests = {
    # type name : (Test, DowngradeTo, StringTypeName, StringTypeNameIfNullable)
    int: ([int], float, "int", "int"),
    float: ([float], datetime, "float", "float"),
    datetime: (
      [
       lambda value: datetime.strptime(value, "%m/%d/%Y"),
       lambda value: datetime.strptime(value, "%Y-%m-%d"),
       lambda value: datetime.strptime(value, "%m/%d/%Y %H:%M:%S"),
       lambda value: datetime.strptime(value, "%Y-%m-%d %I:%M:%S.%f %p")
      ], str, "datetime", "datetime"),    
    str: ([str], str, "varchar(MAX)", "varchar(100)")
}

# default type that is most restrictive to start with
type_most_restrictive = int



# read first 10 rows and use majority vote to determine the deliminator
def get_delim(content_file):
    supported_delims = [k for k, v in delims.items()]
    num_lines_hasSep = {k:0 for k, v in delims.items()}
    total_hasSep = {k:0 for k, v in delims.items()}
    is_header_row = True
    total_rows_read = 0
    num_rows_to_test = 10

    # collect sample rows
    #with open(readFileName, 'r', encoding='utf8') as content_file:
    line = content_file.readline()
    while (line != ''):
    #for line in content_file:
        if (is_header_row) :
            is_header_row = False
        for c in supported_delims:
            cnt = line.count(c)
            num_lines_hasSep[c] += (1 if cnt > 0 else 0)
            total_hasSep[c] += cnt
        total_rows_read+=1
        #print(line)
        if (total_rows_read >= num_rows_to_test) :
            break
        else:
            line = content_file.readline()
    
    # determine delimiters
    # first, each row should have the delim, if one doesn't filter it out
    survive_delim = {key:value for key, value in num_lines_hasSep.items() if value >= total_rows_read}
    #print(survive_delim)
    if (len(survive_delim) <= 0):
        # no deliminator? should be one row case. we use default as delimiter
        return supported_delims[0]
    else:
        return sorted(survive_delim.items(), key=operator.itemgetter(1), reverse=True)[0][0]


def regulate_token(str):
    # for now regulating column name means remove all splitter strings, and replace , with _
    return str.replace(" ", "").replace(",", "_")

def get_mostprefered_type(curtype, value):
    typ = curtype
    while (typ != str):
        typobj = type_tests[typ]
        for typtest in typobj[0]:
          try:
              typtest(value)
              return typ
          except ValueError:
              # try next
              pass
        typ = typobj[1]
    return str

def get_type_rep(type, isnullable, iskey):
    if (iskey):
        return f"""{type_tests[type][3]}{" NULL" if isnullable else ""}"""
    else:
        return f"""{type_tests[type][2]}{" NULL" if isnullable else ""}"""


# read a string file and extract columns and label it with types
# return (cols, rows)
def extract_from_csv(content_file, delim, has_header): 
    # read the csv file row by row
    reader = csv.reader(content_file, delimiter=delim)
    
    num_cols = 0
    rowid = 0
    colmap = {}
    rows = []

    for row in reader:
        rowid+=1
        if (num_cols == 0):
            num_cols = len(row)
            # seen first row, create column definitions
            if (has_header):
                for id, col in enumerate(row):
                    colmap[id] = {'colname':col, 'type':type_most_restrictive, 'nullable':False, 'confirmed':False}  # col name, type, nullable?, confirmed?
                # header read. move to next line
                continue
            else:
                for id, col in enumerate(row):
                    regname = regulate_token(col)
                    colmap[id] = {'colname':("Column%d" % (id+1)), 'type':type_most_restrictive, 'nullable':False, 'confirmed':False}
        elif len(row) == 0:
            # skip empty rows
            continue
        elif num_cols != len(row):
            #col inconsistent
            raise ValueError('Line %d has inconsistent number of cols %d, expect %d. Line content:\n%s' % (rowid, len(row), num_cols, row))
        elif (row is not None):
            # see normal row. Fall through to do the common processing
            pass
        else:
            raise Exception("Unhandled situation")
        
        # the common processing: update column type for each column
        for id, col in enumerate(row):
            curT = colmap[id]['type']
            is_confirmed = colmap[id]['confirmed']
            if ( (not is_confirmed) or
                 (curT is not str) ):
                # string cannot be downgraded anymore, other types can
                # unless, we haven't seen the first confirmable data yet
                if (col == ''):
                    # col is nullable
                    colmap[id]['nullable'] = True
                else:
                    # check the type
                    colmap[id]['type'] = get_mostprefered_type(curT, col)
                    colmap[id]['confirmed'] = True

        rows.append(row)
    
    return colmap, rows

# helper function to parse boolean
def parse_bool(s):
    return (s in ['true', '1', 't', 'y', 'yes'])


def create_table_insert(table_name, colmap, pkcols):
    script = f"DROP TABLE IF EXISTS dbo.{table_name};\nGO;\n"
    script += f"CREATE TABLE dbo.{table_name}\n"
    script += "(\n"
    # construct the type string
    cols = sorted(colmap.items(), key=operator.itemgetter(0))
    first = True
    for col in cols:
        script += f"""    {"" if first else ", "}{col[1]['colname']} {get_type_rep(col[1]['type'], col[1]['nullable'], col[1]['colname'] in pkcols)}\n"""
        first = False
    pkcols_exists = [ pkcol for pkcol in pkcols if pkcol in [col[1]['colname'] for col in cols] ]
    if len(pkcols_exists) > 0:
        script += f"""    , CONSTRAINT PK_{table_name.upper()}\n"""
        script += f"""    PRIMARY KEY CLUSTERED ({", ".join(pkcols_exists)})\n"""
        script += f"""    WITH (IGNORE_DUP_KEY = OFF)\n"""
    script += ");\nGO;\n"
    return script

def formatvalue(val, coltype):
    if coltype is str:
        return f"""'{val.replace("'", "''")}'"""
    else:
        return "NULL" if val is None or val == "" else val

def create_insert(table_name, colmap, rows):
    script = ""
    cols = sorted(colmap.items(), key=operator.itemgetter(0))
    for row in rows:
        script += f"""INSERT INTO {table_name} ({", ".join([col[1]['colname'] for col in cols])})\n"""
        row_quoted = [ formatvalue(row[i], cols[i][1]['type']) for i in range(len(cols)) ]
        script += f"""    VALUES ({", ".join(row_quoted)})\n"""
	script += "GO;\n"
    return script

# Test code
def run_test() :
    # expect ,
    testFile = "A,B,C\n1,str,str2\n2,str3,str4"
    testFileDummy = StringIO(testFile)
    testres = get_delim(testFileDummy)
    print("delimiter is %s" % testres)
    assert(testres == ',')

    # expect <tab>
    testFile = "A\tB\tC\n1\tstr\tstr2\n2\tstr3\tstr4"
    testFileDummy = StringIO(testFile)
    testres = get_delim(testFileDummy)
    print("delimiter is %s" % '<\\t>' if testres == '\t' else get_delim(testFileDummy))
    assert(testres == '\t')

    # expect <tab>
    testFile = "A\tB,C\tD\n1\tstr\tstr2\n2\tstr3,str4\tstr5"
    testFileDummy = StringIO(testFile)
    testres = get_delim(testFileDummy)
    print("delimiter is %s" % '<\\t>' if testres == '\t' else get_delim(testFileDummy))
    assert(testres == '\t')


def run_test2() :
    testStr = "my col name"
    testres = regulate_token(testStr)
    print("%s --> %s" % (testStr, testres))
    assert(testres == 'mycolname')

    testStr = "my col name, name2"
    testres = regulate_token(testStr)
    print("%s --> %s" % (testStr, testres))
    assert(testres == 'mycolname_name2')


def run_test3() :
    
    #expect: A:int, B:str, C:str
    testFile = "A,B,C\n1,str,str2\n2,str3,str4"
    testFileDummy = StringIO(testFile)
    testres, testrows = extract_from_csv(testFileDummy, ',', True)
    print(testres)
    assert(len(testres.items()) == 3)
    # TODO: more asserts

    #expect: Column1:int, Column2:str, Column3:str
    testFile = "1,str,str2\n2,str3,str4"
    testFileDummy = StringIO(testFile)
    testres, testrows = extract_from_csv(testFileDummy, ',', False)
    print(testres)
    assert(len(testres.items()) == 3)
    # TODO: more asserts


# read a file, filter out the bad chars in memory, write the cleaned file back
def process(csv_file, has_header, outtable, outfile):
    print(f"Reading {csv_file} ...")
    print(f"""The CSV file should{"" if has_header else " not"} contain header""")

    # peek the csv file and mine the column type
    colmap = {}
    with open(csv_file, 'r', encoding='utf8') as content_file:
        delim = get_delim(content_file)

    # TODO: this is not optimal, we can do one pass instead of 2 pass here
    with open(csv_file, 'r', encoding='utf8') as content_file:
        colmap, rows = extract_from_csv(content_file, delim, has_header)

    # Generate table creation statement
    table_name = outtable
    full_script = create_table_insert(table_name, colmap, ["id", "_vertexId", "_sink"])
    full_script += create_insert(table_name, colmap, rows)

    with open(outfile, 'a', encoding='utf8') as target_file:
        target_file.write(full_script)

    print("Done")


def main():

    if len(sys.argv) < 2 :
        print("Error: must provide the csv/tsv file name to peek at")
        return
  
    if len(sys.argv) < 3 :
        print("Error: must provide true or false that the csv file contains header")
        return

    if len(sys.argv) < 4 :
        print("Error: must provide a table name file to write to")
        return

    if len(sys.argv) < 5 :
        print("Error: must provide a script file to write to (or append to)")
        return
        
    csv_file = sys.argv[1]
    has_header = parse_bool(sys.argv[2])
    out_table = sys.argv[3]
    out_file = sys.argv[4]

    process(csv_file, has_header, out_table, out_file)


# main operations

#run_test()
#run_test2()
#run_test3()
main()
