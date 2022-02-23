import pandas as pd
import numpy as np
import sqlite3
import plotly.express as px
import plotly.io as pio
pio.templates.default="seaborn"


class VaccinesDB:
    """ read a csv file, transform a data, create a database
        - path_str (str) - path to a csv file
        - df_name (str) - give a name to a new database
        - conn - create a connection with a database
        - cur - create a cursor 
    """ 
    def __init__(self, path: str, db_name=""):
        """ initialize VaccinesDB
        Params: path (str), db_name (str)
        """
        self.df = pd.read_csv(path)
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)
        self.cur = self.conn.cursor()
          
    def transform_df(self, col, col_vars, new_name, col_s, col_e):
        """ separate a column with comma-separated values to several columns
        and convert a categorical variable into dummy/indicator variables 
        (one-hot encoding), concatinate a dataframe with new columns, 
        create a new dataframe where columns become variables (melt function)
        Args: col (str), col_vars (str), new_name (str), col_s (int), col_e (int)
        """
        self.df = pd.concat([self.df, self.df[col].str.get_dummies(sep=", ")], axis=1)
        # replace the bad symbols with an underscore
        self.df.columns= self.df.columns.str.replace(" |&|-|/", "_", regex=True)
        # select and concatinate from a dataframe to a new dataframe
        self.df_melt = pd.concat([self.df.iloc[:,1], self.df.iloc[:,col_s:col_e]], axis=1)
        self.df_melt = self.df_melt.melt(id_vars=[col_vars],\
             value_vars = self.df.iloc[:,col_s:col_e],var_name =new_name)
        # delete duplicated rows
        self.df_melt = self.df_melt[self.df_melt.value != 0].drop_duplicates(keep="last")
 
    
    def create_table(self, table, query):
        """ control if a table in the database exists and create a table
        Args: table (str), query (str)
        """
        self.cur.execute('''DROP TABLE IF EXISTS ''' + table)
        self.cur.execute('''CREATE TABLE IF NOT EXISTS ''' + table + 
                            '''(ID INTEGER PRIMARY KEY AUTOINCREMENT,''' + query +
                            ''')''')

    def load_to_table(self, table, columns):
        """ insert values into a table from a dataframe
        Args: table (str), columns (list)
        """
        self.cur
        self.df[columns].\
        to_sql(table, index=False, if_exists="append", con=self.conn)
    
    def load_to_table_vaccines(self,  table, columns):
        """ insert values into a table from the transformed dataframe
        Args: table (str), columns (list)
        """
        self.cur
        self.df_melt[columns].\
        to_sql(table, index=False, if_exists="append", con=self.conn)

    def load_to_table_pk_drop_dub(self, table, columns):
        """ insert values into a table from a dataframe
        delete dublicated rows, set a primary key
        Args: table (str), columns (list)
        """
        self.cur
        self.df[columns].drop_duplicates(keep="last").\
        to_sql(table, index=False, if_exists="replace",\
            con=self.conn, dtype={"iso_code": 'PRIMARY KEY'})
    
    def delete_db_nan(self, table, condition):
        """ delete from a database values with a condicition
        Args: table (str), condition (str)
        """
        self.cur.execute('''DELETE FROM ''' + table  +
                    ''' WHERE ''' + condition + '''''')
        self.conn.commit()
    
    def update_db_na_to_zero(self, table, new_value, condition):
        """ update a database with new values with a condicition
        Args: table (str), new_value (str), condition (str)
        """
        self.cur.execute('''UPDATE ''' + table  + ''' SET ''' + new_value +
                    ''' WHERE ''' + condition + '''''')
        self.conn.commit()
    
    def close_cursor(self):
        """ close  SQLiteCursor cursor
        Args: None
        """
        self.cur.close()
    

class Visualization(VaccinesDB):
    """ the child class "Visualization" that inherits all from the parent class "VaccinesDB"
    """ 
    def query_to_plot(self):
        """ create two dataframes from SQL-query
        the main dataframe is used to make a plot
        the second dataframe is used to make annotations on the plot
        """
        self.conn
        # a SQL-query to the main dataframe
        self.df_subset = pd.read_sql('''WITH geoplot
                    AS (
                        SELECT iso_code,
                               COUNT(vaccine) AS number_of_used_vaccines
                        FROM vaccines
                        GROUP BY iso_code)
                        SELECT 
                            c.country,
                            p.iso_code,
                            strftime('%m/%Y', p.date) AS date,
                            p.people_fully_vaccinated_per_hundred 
                                    AS people_fully_vaccinated_per_100,
                            t.total_vaccinations_per_hundred 
                                    AS total_vaccinations_per_100,
                            g.number_of_used_vaccines 
                        FROM people p 
                        JOIN  geoplot AS g
                        JOIN countries c
                        JOIN total t
                        ON p.iso_code = g.iso_code 
                           and c.iso_code = p.iso_code 
                           and p.ID = t.ID
                        WHERE p.date > "2020-12-31" 
                        ORDER BY p.date ASC''', self.conn)
        # replace the bad symbols with space to get better names on the plot
        self.df_subset.columns = self.df_subset.columns.str.replace("_", " ",regex=True)
        # a SQL-query to the annotation dataframe
        self.df_annotation = pd.read_sql('''WITH a
                    AS
                    (
                    SELECT
                    iso_code, 
                    MAX(date) AS date,
                    people_fully_vaccinated_per_hundred AS people
                    FROM people
                    GROUP BY iso_code)
                    SELECT a.iso_code || ' ' || strftime('%m/%Y', a.date) 
                           AS LastUpdate
                    FROM a a
                    JOIN people p
                    ON a.iso_code = p.iso_code
                    WHERE a.date < "2022-02-01" 
                    GROUP BY a.iso_code
                    ORDER BY a.date ASC''', self.conn)
        

def transform_db(df):
    """ call the transform_df method to update a dataframe
    Params: df (pandas.DataFrame)
    Return: None
    """ 
    df.transform_df(col="vaccines", col_vars="iso_code",\
                    new_name="vaccine", col_s=19, col_e=42)

def load_db(df):
    """ call the create_table, load_to_table, load_to_table_vaccines,
    load_to_table_pk_drop_dub methods to load the data into the database
    Params: df (pandas.DataFrame)
    Return: None
    """ 
    # create a table with daily vaccinations and insert values from a dataframe
    df.create_table("daily", "iso_code TEXT, date INTEGER,\
                    daily_vaccinations INTEGER,\
                    daily_vaccinations_per_million INTEGER,\
                    FOREIGN KEY (iso_code) REFERENCES countries\
                    (iso_code) ON DELETE CASCADE\
                    ON UPDATE NO ACTION")
    df.load_to_table("daily", ["iso_code", "date", "daily_vaccinations",\
                    "daily_vaccinations_per_million"])
    # create a table with number of vaccinated people 
    # and insert values from a dataframe     
    df.create_table("people", "iso_code TEXT,\
                    date INTEGER,\
                    people_vaccinated INTEGER,\
                    people_fully_vaccinated INTEGER,\
                    people_vaccinated_per_hundred INTEGER,\
                    people_fully_vaccinated_per_hundred INTEGER,\
                    FOREIGN KEY (iso_code) REFERENCES countries\
                    (iso_code) ON DELETE CASCADE\
                    ON UPDATE NO ACTION")
    df.load_to_table("people", ["iso_code","date", "people_vaccinated",\
                    "people_fully_vaccinated","people_vaccinated_per_hundred",\
                    "people_fully_vaccinated_per_hundred"])
    # create a table with total number of vaccinations 
    # and insert values from a dataframe 
    df.create_table("total", "iso_code TEXT, date INTEGER,\
                    total_vaccinations INTEGER,\
                    total_vaccinations_per_hundred INTEGER,\
                    FOREIGN KEY (iso_code) REFERENCES countries\
                    (iso_code) ON DELETE CASCADE\
                    ON UPDATE NO ACTION")  
    df.load_to_table("total", ["iso_code", "date",\
                    "total_vaccinations","total_vaccinations_per_hundred"])
    # create a table with vaccines and insert values from a dataframe 
    df.create_table("vaccines", "iso_code TEXT, vaccine TEXT,\
                    FOREIGN KEY (iso_code) REFERENCES countries\
                    (iso_code) ON DELETE CASCADE ON UPDATE NO ACTION")
    df.load_to_table_vaccines("vaccines", ["iso_code", "vaccine"])
    # create a table with number of boosters and insert values from a dataframe 
    df.create_table("booster", "iso_code TEXT, date INTEGER,\
                    total_boosters INTEGER,\
                    total_boosters_per_hundred INTEGER,\
                    FOREIGN KEY (iso_code) REFERENCES countries\
                    (iso_code) ON DELETE CASCADE\
                    ON UPDATE NO ACTION")
    df.load_to_table("booster", ["iso_code", "date",\
                    "total_boosters","total_boosters_per_hundred"])
    # create a table with countries names and iso codes 
    # and insert values from a dataframe 
    df.load_to_table_pk_drop_dub("countries",\
                                ["iso_code", "country"])
    # create a table with data sources and insert values from a dataframe 
    df.load_to_table_pk_drop_dub("source",\
                                ["iso_code", "source_name", "source_website"])

def clean_in_db(df):
    """ call the delete_db_nan and pdate_db_na_to_zero methods
    to delete NaNs and update the database
    Params: df (pandas.DataFrame)
    Return: None
    """ 
    # delete NaN and zero-values 
    df.delete_db_nan("total",\
                     "total_vaccinations IS NULL OR total_vaccinations = 0")
    df.delete_db_nan("people",\
                     "people_vaccinated IS NULL OR people_vaccinated = 0")
    df.delete_db_nan("booster",\
                     "total_boosters IS NULL OR total_boosters = 0")
    # convert NaN into "0"
    df.update_db_na_to_zero("people", "people_fully_vaccinated = 0,\
                             people_fully_vaccinated_per_hundred = 0",\
                             "people_fully_vaccinated IS NULL")
    df.update_db_na_to_zero("daily", "daily_vaccinations = 0,\
                             daily_vaccinations_per_million = 0",\
                             "daily_vaccinations IS NULL")
    
def plot(df):
    df.query_to_plot() # call the dataframes
    # subset the title into four pieces because it is too long
    title_1 = "People Fully Vaccinated and Total Vaccinations per hundred,<br>"
    title_2 = "and Number of Used Vaccines per Country,"
    title_3 = " last update 19 February 2022<br>"
    subtitle = "<sub>hover over a circle to see the data</sub>"
    longtitle = title_1 + title_2 + title_3 + subtitle
    # create a layout Choropleth from Plotly
    geo = px.choropleth(df.df_subset, 
                locations="iso code", 
                projection="natural earth", # select a map
                color="people fully vaccinated per 100", 
                color_continuous_scale="rdylbu", # set up a color palette
                labels = {"people fully vaccinated per 100":\
                          "people fully <br>vaccinated per 100"},
                animation_frame="date", # add data intervals
                title=longtitle,
                range_color=(0, 100)) # fix a legend range
    # create a layout Scattergeo from Plotly
    geo2 = px.scatter_geo(df.df_subset, 
                locations="iso code",
                hover_name="country", # a column which is displayed as the tooltip title
                # the columns to a hover label
                hover_data = {"iso code": False, "date": False,
                            "people fully vaccinated per 100": True,
                            "number of used vaccines": True,
                             "total vaccinations per 100": True},
                size="number of used vaccines",
                animation_frame="date") # add data intervals
    # set a new color and size of markers on the layout Scattergeo
    marker_geo2 = dict(size=10, color = "SteelBlue",
                              line=dict(width=1,
                                        color='white'))
    # update color and size of the markers 
    # on the layout Scattergeo for all the dates
    geo2.update_traces(marker=marker_geo2) 
    for frame in geo2.frames:
        frame.data[0].marker = marker_geo2  
    # delete the markers on the layout Choropleth
    hovertemplate_geo = "<b>""<b>"            
    geo.update_traces(hovertemplate = hovertemplate_geo)
    for frame in geo.frames:
        frame.data[0].hovertemplate = hovertemplate_geo
    # set an annotations title
    geo.add_annotation(text="LAST UPDATE:",
        xref="paper", yref="paper",
        font = {'family': "sans-serif", 'size': 9,\
                'color': "MidnightBlue"},
                x=0.05, y=0.025, showarrow=False)
    # place the annotations as the columns on the layout
    n_rows = len(df.df_annotation)
    s, e = 0, 4 # a start and end index to iloc
    x_coor = 0.05 # x-coordinate to place the first column
    while s < n_rows:
        y_coor = -0.02 # y-coordinate to place the first column
        for index, row in df.df_annotation.iloc[s:e].iterrows():
            geo.add_annotation(text=row['LastUpdate'],
                                    xref="paper", yref="paper",
                                    font = {"family": "sans-serif", 'size': 9,\
                                    "color": "MidnightBlue"},
                                    x = x_coor, y = y_coor, showarrow=False)
            y_coor -= 0.02
        x_coor += 0.08
        s += 4
        e += 4
    # combine the first and second layouts
    geo.add_trace(geo2.data[0])
    for i, j in enumerate(geo.frames):
        geo.frames[i].data += (geo2.frames[i].data[0],)
    # show the plot
    geo.show()


def main():
    """ The main function
        1. Read the "vaccin_covid.csv" file to a pandas DataFrame, 
           create the SQLite-database "Vaccinations_world"
        2. Transform the dataframe to get separate columns from the column
           "vaccines" with the comma-separated values
        3. Create the tables in the database, and load the data 
           from the dataframe to the database
        4. Delete and/or update NaN with the SQL-queries
        5. Create a plot which opens automatically in Google Chrome.
           The data is stored in the cirlcles. To read the data, hover over a circle.
        6. Close a SQLiteCursor cursor
    Parameters: None   
    Return: None
    """
    df_vacc_test = Visualization("vaccin_covid_feb2022.csv", "Vaccinations_world.db") # 1
    transform_db(df_vacc_test) # 2
    load_db(df_vacc_test) # 3
    clean_in_db(df_vacc_test) # 4
    plot(df_vacc_test) # 5
    df_vacc_test.close_cursor() #6
    
if __name__ == "__main__":
    main()