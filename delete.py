import os
# DELETE CSV FILES
def delete_files(file):
    print("Starting delete files ",file)

    os.remove(file)
    #Remove files
    
    print("Deleted file",file)
    
delete_files("test.txt")