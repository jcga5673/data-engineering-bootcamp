import pandas as pd

# Path of csv downloaded
csv_file = r"D:\profeco\all_data.csv"
# Size of chunks to analize csv dataframe
chunksize = 10_000_000
# Name of columns to use (omiting 80% of data using only 3 of 15 columns)
columns=['cadenaComercial','producto','estado']
### Question the 10 commercial chains with more products monitored and whats
### their percentage of products 
# Calculate number of rows in csv (So we can use the result to calculate percentages)
number_lines = sum(1 for row in (open(csv_file,encoding="utf8")))
# read csv into chunks and only using the columns referenced before
csv = pd.read_csv(csv_file, sep=',', chunksize=chunksize,
                         low_memory=False,usecols=columns)
# For that will help us to proccess one chunk at a time
for i, chunk in enumerate(csv):
    # Just a print that will help us to see where the program is
    print('Chunk: ', i)
    # To initialize the dataframes that will help us to answer the questions
    if i==0:
        # df for answer 1
        # Separate first dataframe just by one column (the desire one)
        df_answer1 = chunk['cadenaComercial']
        # Start to drop the duplicates to optimize the dataframe
        df_answer1 = df_answer1.drop_duplicates().reset_index(drop=True)
        #df for answer 2
        # Separate second dataframe just by the two columns we want to analyze
        df_answer2= chunk[['producto','estado']]
        # Start counting the products repetead by each state ('estado')
        df_answer2 = df_answer2.groupby(['producto','estado']).size().reset_index(name='count')
        #df for answer 3
        # Separate third dataframe just by the two columns we want to analyze
        df_answer3= chunk[['producto','cadenaComercial']]
        # Start counting the products repetead by each commercial chain ('cadenaComercial')
        df_answer3 = df_answer3.groupby(['producto','cadenaComercial']).size().reset_index(name='count') 
        #df for answer 4 
        # Separate fourth dataframe just by the two columns we want to analyze
        df_answer4= chunk[['estado','cadenaComercial']]
        # Start counting the times a commercial chain ('by state') is repetead
        df_answer4 = df_answer4.groupby(['estado','cadenaComercial']).size().reset_index(name='count') 
    else:
        #Operations for answer 1
        # Concatenate the answer from previous chunk with actual chunk
        df_answer1 = pd.concat([df_answer1,chunk['cadenaComercial']])
        # Drop duplicates
        df_answer1 = df_answer1.drop_duplicates().reset_index(drop=True)
        # Operations for answer 2
        # Separate second dataframe just by the two columns we want to analyze
        df_2 = chunk[['producto','estado']]
        # Keep counting the products repetead by each state
        df_2 = df_2.groupby(['producto','estado']).size().reset_index(name='count')
        # Concatenate the answer from previous chunk with actual chunk
        df_answer2 = pd.concat([df_answer2, df_2]).groupby(['producto','estado']).sum().reset_index()
        #Operations for answer 3
        # Separate second dataframe just by the two columns we want to analyze
        df_3 = chunk[['producto','cadenaComercial']]
        # Keep counting the products repetead by each commercial chain
        df_3 = df_3.groupby(['producto','cadenaComercial']).size().reset_index(name='count')
        # Concatenate the answer from previous chunk with actual chunk
        df_answer3 = pd.concat([df_answer3, df_3]).groupby(['producto','cadenaComercial']).sum().reset_index()
        #Operations for answer 4
        # Separate second dataframe just by the two columns we want to analyze
        df_4 = chunk[['estado','cadenaComercial']]
        # Keep counting the times a commercial chain is repetead
        df_4 = df_4.groupby(['estado','cadenaComercial']).size().reset_index(name='count')
        # Concatenate the answer from previous chunk with actual chunk
        df_answer4 = pd.concat([df_answer4, df_4]).groupby(['estado','cadenaComercial']).sum().reset_index()
        
        
### Solution of answer 1
# Print answer 1 by counting the len of the df
answer1 = len(df_answer1.index)
print('number of commercial chains in the database: ', len(df_answer1.index))
### Solution of answer 2
# Sorting the results of df_answer2 and just collect first 10 values of every state
answer_2 = df_answer2.sort_values(by=['count'], ascending=False).groupby('estado').head(10)
answer_2 = answer_2.sort_values(by=['estado','count'],ascending=False).reset_index(drop=True).iloc[1:]
# Save answer into csv 
print('Loading answer of question 2 on answer2.csv...')
answer_2.to_csv(r"D:\profeco\answer2.csv") 
### Solution of answer 3
# Print the top result of the result of the third df
answer_3 = df_answer3.sort_values(by=['count'], ascending=False).head(1)
print('The Commercial chain with the highest number of monitored products is: ', answer_3['cadenaComercial'].iloc[0])
### Solution of answer 4
# Gruop by Comercial chain and sorting by count to save best 10 values into a new df
df_answer4 = df_answer4[['cadenaComercial','count']].groupby('cadenaComercial').sum()
df_answer4 = df_answer4.sort_values(by=['count'],ascending=False).head(10)
# Calculate the percentage of top 10 commercial chains (by number of products)
products_answer = df_answer4['count'].sum()
percentage = round((products_answer/(number_lines))*100,2)
# Print df of answer 4
print(df_answer4)
df_answer4.to_csv(r"D:\profeco\answer4.csv") 
# Print the percentage of answer 4
print('Percentage of products in the top 10 commercial chains: %', percentage)

