import argparse
import csv
import datetime
import magic
import pandas
import pymongo
import sys
import warnings
warnings.filterwarnings("ignore")


def parse_args():
    parser = argparse.ArgumentParser(description="Submit QA csv/excel to EG database.")
    # Destination
    parser.add_argument("-l", "--local", help="Send to Collection 1.", action="store_true")
    parser.add_argument("-m", "--mega", help="Send to Collection 2.", action="store_true")
    # Calls
    parser.add_argument("-s", "--send", help="Send reports to a collection", action="store")
    parser.add_argument("-a", "--all", help="Return all reports (overrides all arguments)", action="store_true")
    parser.add_argument("-p", "--special", help="Return First, Middle, and Last Test #", action="store_true")
    parser.add_argument("-b", "--blocker", help="Return blockable tests", action="store_true")
    parser.add_argument("-d", "--build", help="Return tests by build date", action="store")
    parser.add_argument("-r", "--repeatable", help="Return repeatable tests", action="store_true")
    parser.add_argument("-u", "--user", help="Return cases by test owner", action="store")
    # Etc
    parser.add_argument("-c", "--csv", help="Save queries to a csv", action="store_true")
    return parser.parse_args()


def readFile(file):
    filetype = magic.from_file(file)
    if filetype in ["CSV text", "ASCII text"]: 
        return pandas.read_csv(file, keep_default_na=False, na_values="")
    elif filetype == "Microsoft Excel 2007+": 
        return pandas.read_excel(file, keep_default_na=False, na_values="")
    else: 
        print("Incorrect file type. Use CSV or Excel files.")
        sys.exit()  


def cleanLine(df):
    df = df.dropna() # Drop rows with empty cells 
    #df = df.drop_duplicates(subset=df.iloc[:, 3:8]) # Remove dupes based on test case, expected, actual
    df = df.drop_duplicates()
    # Check test numbers as integers > 0
    df.iloc[:, 0] = pandas.to_numeric(df.iloc[:, 0], errors="coerce")
    df = df[df.iloc[:, 0] > 0].astype({df.columns[0]: int})
    # Check valid dates
    df.iloc[:, 1] = pandas.to_datetime(df.iloc[:, 1], errors="coerce")
    df = df[(df.iloc[:, 1] >= pandas.Timestamp("01/01/2024")) & (df.iloc[:, 1] <= pandas.Timestamp("05/31/2024"))]

    if len(df) == 0:
        print("No valid reports recovered. Program ending...")
        sys.exit()

    print("Reports recovered:", len(df))
    return df


def main():
    client = pymongo.MongoClient(["mongodb://localhost:27017/"])
    db = client["TheReckoning"]
    col1 = db["EG_Local"]
    col2 = db["EG_Mega"]
    curr_col = None 
    columns = ["Test #", "Build #", "Category", "Test Case", "Expected Result", "Actual Result", "Repeatable?", "Blocker?", "Test Owner"] # Should be gathered by saving header
    stamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S") # For saving CSV files
    query = {} # For mongodb calls   .
    args = parse_args() 
    # Check server connection
    try:
        client.server_info()
    except pymongo.errors.ConnectionFailure:
        print(" /\\_/\\\n(='w'=)\num i cant find da server")
        sys.exit()
    # Check destination
    if args.local == True: curr_col = col1
    elif args.mega == True: curr_col = col2
    else:        
        print("Please select --local or --mega destination.")
        sys.exit()
    # Clean up files and send them to collections
    if args.send:
        df = readFile(args.send)
        df = cleanLine(df) # Remove broken data
        # Upload, check for duplicates in server
        try:
            for row in df.to_dict(orient="records"):
                curr_col.update_one(filter=row, update={"$set": row}, upsert=True)
        except: 
            print("There was some problem uploading to the server. Program ending...")
            sys.exit()
    if args.all == True:
        requests = list(curr_col.find(query, {"_id": 0}))
        df = pandas.DataFrame([{key: request.get(key, None) for key in columns} for request in requests])
        print(df)
        if args.csv == True: df.to_csv(f"TheReckoning{stamp}.csv", index=False, quoting=csv.QUOTE_ALL, sep=";")
        sys.exit()
    if args.special == True:
        df = pandas.DataFrame(list(curr_col.find(query, {"_id": 0})))
        first = df.head(1)
        middle = df[len(df) // 2 - 1:len(df) // 2]
        last = df.tail(1)
        df = df[columns]
        df = pandas.concat([first, middle, last], axis=0)
        print(df)
        if args.csv == True: 
            df.to_csv(f"TheReckoning{stamp}.csv", index=False, quoting=csv.QUOTE_ALL, sep=";")  
        sys.exit()
    # Find positive blocker cases
    if args.blocker == True: query["Blocker?"] = {"$regex": "\\b(?:yes|Y)(?!/no)\\b", "$options": "i"}
    # Find cases by build date
    if args.build: query["Build #"] = pandas.to_datetime(args.build)
    # Find positive repeatable cases
    if args.repeatable == True: query["Repeatable?"] = {"$regex": "\\b(?:yes|Y)(?!/no)\\b", "$options": "i"}
    # Find cases by test owner
    if args.user: query["Test Owner"] = args.user
    
    # Fix up formatting
    requests = list(curr_col.find(query, {"_id": 0}))
    df = pandas.DataFrame([{key: request.get(key, None) for key in columns} for request in requests])
    # Print and saving 
    print(df)    
    if args.csv == True: 
        df.to_csv(f"TheReckoning{stamp}.csv", index=False, quoting=csv.QUOTE_ALL, sep=";")

if __name__ == '__main__': main()