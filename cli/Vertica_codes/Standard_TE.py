
import vertica_db_client
import sys
import timeit 
	

#Connection to vertica database
db = vertica_db_client.connect("database=graphdb port=5433 user=dbadmin")
cur = db.cursor()

graph_file = sys.argv[1]
outputpath = sys.argv[2]
undirected = sys.argv[3].lower()

filepath = graph_file.split("/")
filename = filepath[len(filepath)-1]

#Read data
print("Reading data ...")

start_time = timeit.default_timer()
gf = open(graph_file, "r")
cur.stdin = gf
cur.execute("DROP TABLE IF EXISTS E_s CASCADE;")
cur.execute("CREATE TABLE E_s(i int NOT NULL,j int NOT NULL);")
cur.execute("COPY E_s FROM STDIN DELIMITER AS ' '", gf)

if (undirected=="undirected"):
	cur.execute("INSERT INTO E_s SELECT j,i FROM E_s;")
cur.execute("COMMIT;")

elapsed_time = timeit.default_timer() - start_time

print("Loading graph data took: " + str(elapsed_time) + " seconds\n")

print("Triangle Enumeration ...")

start_time = timeit.default_timer()
time = 0
cur.execute("SELECT E1.i AS v1, E1.j AS v2, E2.j AS v3 FROM E_s E1 JOIN E_s E2 ON E1.j=E2.i \
 JOIN E_s E3 ON E2.j=E3.i WHERE E1.i<E1.j AND E2.i<E2.j AND E1.i=E3.j ORDER BY v1,v2,v3;")
if cur.rowcount != 0:
	rows = cur.fetchall()
	time1 = timeit.default_timer() - start_time
	print("Triangle enumeration took: " + str(time1) + " seconds")
	print("Total time is : " +str(elapsed_time+time1) + " seconds")
	print("Total Triangle count is: " + str(cur.rowcount))
	with open(outputpath+"/Standard_"+filename, 'w+') as triangles:
		for i, row in enumerate(rows):
			triangles.write(str(row)+'\n')
