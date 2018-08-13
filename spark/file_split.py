import gzip, bz2
import os
import fnmatch
import argparse
import boto3

def gen_find():
    yield os.path.join("/home/ubuntu/sourcefile/raw","wiki.main.bz2")

def gen_open(filenames):
    for name in filenames:
        if name.endswith(".gz"):
            yield gzip.open(name)
        elif name.endswith(".bz2"):
            yield bz2.BZ2File(name)
        else:
            yield open(name)

def gen_cat(sources):
    for s in sources:
        for item in s:
            yield item

def main(client):
    fileNames = gen_find()
    fileHandles = gen_open(fileNames)
    fileLines = gen_cat(fileHandles)
    lc=0
    fc=1
    for line in fileLines:
        if line.startswith("REVISION"):
		lc+=1
		if lc==10000000:
			client.upload_file("/home/ubuntu/sourcefile/data/wikieditfile"+str(fc)+".txt",'wikieditlog-anamoly',"sourcefile/data/wikieditfile"+str(fc)+".txt")
			os.remove("/home/ubuntu/sourcefile/data/wikieditfile"+str(fc)+".txt")
			fc+=1
			lc=0
		f = open(os.path.join("/home/ubuntu/sourcefile/data","wikieditfile"+str(fc)+".txt"), "a")
                f.write(line)

if __name__ == '__main__':
    #parser = argparse.ArgumentParser(description='Search globbed files line by line', version='%(prog)s 1.0')
    #parser.add_argument('regex', type=str, default='*', help='Regular expression')
    #parser.add_argument('searchDir', , type=str, default='.', help='list of input files')
    #args = parser.parse_args()
    client = boto3.client('s3')
    main(client)
