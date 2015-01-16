import os


def comine_states(state_name):
    '''
    Given a state name this function will go through all the
    CSVs in the directory and combine them if they contain the
    state name. This program basically assumes that you downloaded
    a bunch of WQP state data mimetype=csv and want to aggregate them.
    '''
    #Create the out file first.
    csv_out = open(state_name+'.txt', 'a')
    counter = 1
    #Goes through all the csv files in the directory where this script is executed.
    for file_name in os.listdir(os.getcwd()):
        val_list = file_name.split('.')
        #Only pick out the state relevant csv files
        if state_name in len(val_list) and val_list[1] == 'csv':
            #Get the headers from the first file
            if counter == 1:
                for line in open(file_name, 'r'):
                    csv_out.write(line)
            #Exclude headers in all subsequent files
            else:
                fill_file = open(file_name, 'r')
                fill_file.next()
                for line in fill_file:
                    csv_out.write(line)
                fill_file.close()
            #Here is a nice little counter if you want to track from print or log  
            counter += 1
       csv_out.close()
       return counter
        
        
