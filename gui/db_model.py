"""
    Connect to a vertica database and run queries
"""
from flask import g
from db_util import *
import timeit

def get_db():
	print("Getting db_connection \n")
	db = getattr(g, '_database', None)
	if db is None:
		db = g._database = connect_to_db()
	return db

def get_time():
	"""
		This function return execution time of a query
	"""
	time = 0;
	sql = "SELECT last_statement_duration_us / 1000000.0 last_statement_duration_seconds FROM current_session;"
	exe_time = query_db(sql, db=get_db(), pretty_print=False)
		
	for i,t in enumerate(exe_time):
		time = float(t[0])	
	return time

def standard_te(ds_path, gtype):
	"""
        	Standard triangle enumeration algorithm 
	"""
	print("\n\nStandard Triangle Enumeration \n\n")
	
	sql = "SELECT CLEAR_CACHES();"
	results = query_db(sql, db = get_db(), pretty_print=False)
	
	sql = "DROP TABLE IF EXISTS E_s CASCADE;"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time = get_time()
	
	sql = "CREATE TABLE E_s(i int NOT NULL,j int NOT NULL);"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time += get_time()
	
	sql = "COPY E_s (i, j) FROM STDIN DELIMITER AS ' ';"
	results = load_db(sql, ds_path, db = get_db())
	elapsed_time += get_time()
	
	if (gtype=="undirected"):
		sql = "INSERT INTO E_s SELECT j,i FROM E_s;"
		results = query_db(sql, db = get_db(), pretty_print=False)
		elapsed_time += get_time()
	
	sql = "SELECT local_node_name(), COUNT(*) FROM E_s E1 JOIN E_s E2 ON E1.j=E2.i JOIN E_s E3 ON E2.j=E3.i WHERE E1.i<E1.j AND E2.i<E2.j AND E1.i=E3.j Group By 1 Order By 1;"
	results = query_db(sql,db = get_db(), pretty_print=True)
	elapsed_time += get_time()

	return results,elapsed_time

def randomized_te(ds_path, triplet_path, gtype):
	"""
		Randomized triangle enumeration algorithm
	"""
	print("\n\nRandomized Triangle Enumeration\n\n")
		
	sql = "SELECT CLEAR_CACHES();"
	results = query_db(sql, db = get_db(), pretty_print=False)
	
	sql = "DROP TABLE IF EXISTS E_s CASCADE;"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time = get_time()
	
	sql = "CREATE TABLE E_s(i int NOT NULL,j int NOT NULL) PARTITION BY i;"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time += get_time()

	sql = "CREATE PROJECTION E_s_super(i ENCODING RLE, j ENCODING RLE) AS SELECT i,j FROM E_s ORDER BY i,j SEGMENTED BY hash(i) ALL NODES OFFSET 0 KSAFE 1;"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time += get_time()
	
	sql = "COPY E_s (i, j) FROM STDIN DELIMITER AS ' ';"
	results = load_db(sql, ds_path, db = get_db())
	elapsed_time += get_time()
	
	if (gtype=="undirected"):
		sql = "INSERT INTO E_s SELECT j,i FROM E_s;"
		results = query_db(sql, db = get_db(), pretty_print=False)
		elapsed_time += get_time()

	sql = "DROP TABLE IF EXISTS triplet CASCADE;"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time += get_time()
	
	sql = "CREATE TABLE triplet(machine int,color1 int,color2 int,color3 int);"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time += get_time()

	sql = "CREATE PROJECTION triplet_super(machine, color1, color2, color3) AS SELECT machine, color1,color2,color3 FROM triplet ORDER BY machine UNSEGMENTED ALL NODES;"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time += get_time()
	
	sql = "COPY triplet FROM STDIN DELIMITER AS ',';"
	results = load_db(sql, triplet_path, db = get_db())
	elapsed_time += get_time()

	sql = "DROP TABLE IF EXISTS V_s CASCADE;"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time += get_time()
	
	sql = "CREATE TABLE V_s(i int,color int NOT NULL);"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time += get_time()

	sql = "CREATE PROJECTION V_s_super(i, color ENCODING RLE) AS SELECT i, color FROM V_s ORDER BY i SEGMENTED BY hash(i) ALL NODES OFFSET 0 KSAFE 1;"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time += get_time()
	
	sql = "INSERT INTO V_s SELECT i,randomint(2)+1 FROM (SELECT DISTINCT i FROM E_s UNION SELECT DISTINCT j FROM E_s)V;"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time += get_time()

	sql = "COMMIT;"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time += get_time()

	sql = "DROP TABLE IF EXISTS E_s_proxy CASCADE;"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time += get_time()
	
	sql = "CREATE TABLE E_s_proxy(i_color int NOT NULL,j_color int NOT NULL,i int NOt NULL,j int NOT NULL);"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time += get_time()

	sql = "CREATE PROJECTION E_s_proxy_super(i_color ENCODING RLE, j_color ENCODING RLE, i, j) AS SELECT i_color,j_color,i,j FROM E_s_proxy ORDER BY i,j SEGMENTED BY hash(i_color,j_color) ALL NODES OFFSET 0 KSAFE 1;"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time += get_time()
	
	sql = "INSERT INTO E_s_proxy SELECT Vi.color, Vj.color,E.i,E.j FROM E_s E JOIN V_s Vi ON E.i=Vi.i JOIN V_s Vj ON E.j=Vj.i;"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time += get_time()

	sql = "COMMIT;"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time += get_time()

	sql = "DROP TABLE IF EXISTS E_s_local CASCADE;"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time += get_time()
	
	sql = "CREATE TABLE E_s_local(machine int NOT NULL,i int NOT NULL,j int NOT NULL,i_color int NOT NULL,j_color int NOT NULL);"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time += get_time()

	sql = "CREATE PROJECTION E_s_local_super(machine ENCODING RLE, i, j, i_color ENCODING RLE, j_color ENCODING RLE) AS SELECT machine, i,j, i_color,j_color FROM E_s_local ORDER BY i,j SEGMENTED BY (machine*4294967295//8) ALL NODES OFFSET 0 KSAFE 1;"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time += get_time()
	
	sql = "INSERT INTO E_s_local SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN triplet edge1 ON E.i_color=edge1.color1 and E.j_color=edge1.color2 and E.i<E.j UNION SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN triplet edge2 ON E.i_color=edge2.color2 and E.j_color=edge2.color3 and E.i<E.j UNION SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN triplet edge3 ON E.i_color=edge3.color3 AND E.j_color=edge3.color1 and E.i>E.j;"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time += get_time()

	sql = "COMMIT;"
	results = query_db(sql, db = get_db(), pretty_print=False)
	elapsed_time += get_time()

	sql = "SELECT local_node_name(), COUNT(*) FROM E_s_local E1 JOIN E_s_local E2 ON E1.machine=E2.machine AND E1.j=E2.i JOIN E_s_local E3 ON E2.machine=E3.machine AND E2.j=E3.i WHERE E1.i<E1.j AND E2.i<E2.j AND E1.i=E3.j GROUP BY 1 ORDER BY 1;"
	results = query_db(sql, db = get_db(), pretty_print=True)
	elapsed_time += get_time() 
	return results,elapsed_time

def visualize(path):
	sql = "DROP TABLE IF EXISTS Viz CASCADE;"
	results = query_db(sql, db = get_db(), pretty_print=False)
	
	sql = "CREATE TABLE Viz(i int NOT NULL,j int NOT NULL);"
	results = query_db(sql, db = get_db(), pretty_print=False)
	
	sql = "COPY Viz FROM STDIN DELIMITER AS ' '"
	results = load_db(sql, path, db = get_db())
	
	sql = "SELECT i,j FROM Viz;"
	results = query_db(sql, db = get_db(), pretty_print=False)

	sql = "SELECT DISTINCT i FROM Viz UNION SELECT DISTINCT j FROM Viz;"
	result =  query_db(sql, db = get_db(), pretty_print=False)
	
	return results,result
	

# @app.teardown_appcontext
def close_connection(exception):
	db = getattr(g, '_database', None)
	if db is not None:
		db.close()
