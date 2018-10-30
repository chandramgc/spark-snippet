import sys    
import os    
file_name =  os.path.basename(sys.argv[0])
name = file_name.split('.')
print name[0]
