import vertica_db_client
import sys
import timeit 
	

if __name__ == "__main__":

	#Connection to vertica database
	db = vertica_db_client.connect("database=graphdb port=5433 user=dbadmin")
	cur = db.cursor()

	graph_file = sys.argv[1]
	tripletfile = sys.argv[2]
	outputpath = sys.argv[3]
	undirected = sys.argv[4].lower()

	filepath = graph_file.split("/")
	filename = filepath[len(filepath)-1]

	start_time = timeit.default_timer()

	#Read data
	print("Loading graph data set ...")

	gf = open(graph_file, "r")
	cur.stdin = gf
	cur.execute("DROP TABLE IF EXISTS E_s CASCADE;")
	cur.execute("CREATE TABLE E_s(i int NOT NULL,j int NOT NULL) PARTITION BY i;")
	cur.execute("CREATE PROJECTION E_s_super(i ENCODING RLE, j ENCODING RLE) AS SELECT i,j FROM E_s ORDER BY i,j SEGMENTED BY hash(i) ALL NODES OFFSET 0 KSAFE 1;")
	cur.execute("COPY E_s FROM STDIN DELIMITER AS ' '", gf)

	if (undirected=="undirected"):
		cur.execute("INSERT INTO E_s SELECT j,i FROM E_s;")

	elapsed_time = timeit.default_timer() - start_time
	print("Loading graph Data set took: " + str(elapsed_time) + " seconds \n")
	print("Loading color triplet ...")
	#Random Solution
	start_time = timeit.default_timer()
	tf = open(tripletfile, "r")
	cur.stdin = tf
	cur.execute("DROP TABLE IF EXISTS triplet CASCADE;")
	cur.execute("CREATE TABLE triplet(machine int,color1 int,color2 int,color3 int);")
	cur.execute("CREATE PROJECTION triplet_super(machine, color1, color2, color3) AS SELECT machine, color1,color2,color3 FROM triplet ORDER BY machine UNSEGMENTED ALL NODES;")
	cur.execute("COPY triplet FROM STDIN DELIMITER AS ',';")
	
	triplet_time = timeit.default_timer() - start_time
	elapsed_time += triplet_time
	print("Loading color triplet took: " + str(triplet_time) + " seconds\n")

	#Assign random color to V
	print("Sending edges to proxies ...")
	start_time = timeit.default_timer()
	cur.execute("DROP TABLE IF EXISTS V_s CASCADE;")
	cur.execute("CREATE TABLE V_s(i int,color int NOT NULL);")
	cur.execute("CREATE PROJECTION V_s_super(i, color ENCODING RLE) AS SELECT i, color FROM V_s ORDER BY i SEGMENTED BY hash(i) ALL NODES OFFSET 0 KSAFE 1;")

	cur.execute("INSERT INTO V_s SELECT i,randomint(2)+1 FROM (SELECT DISTINCT i FROM E_s UNION SELECT DISTINCT j FROM E_s)V;")
	cur.execute("COMMIT;")

	V_s_time = timeit.default_timer() - start_time
	elapsed_time += V_s_time

	#Color edges based on colord vertices, edges are repartitioned
	start_time = timeit.default_timer()
	cur.execute("DROP TABLE IF EXISTS E_s_proxy CASCADE;")
	cur.execute("CREATE TABLE E_s_proxy(i_color int NOT NULL,j_color int NOT NULL,i int NOt NULL,j int NOT NULL);")
	cur.execute("CREATE PROJECTION E_s_proxy_super(i_color ENCODING RLE, j_color ENCODING RLE, i, j) AS SELECT i_color,j_color,i,j FROM E_s_proxy ORDER BY i,j SEGMENTED BY hash(i_color,j_color) ALL NODES OFFSET 0 KSAFE 1;")

	cur.execute("INSERT INTO E_s_proxy SELECT Vi.color, Vj.color,E.i,E.j FROM E_s E JOIN V_s Vi ON E.i=Vi.i JOIN V_s Vj ON E.j=Vj.i;")
	cur.execute("COMMIT;")

	E_s_proxy_time = timeit.default_timer() - start_time 
	elapsed_time += E_s_proxy_time

	print("Sending edges to proxies took: " + str(E_s_proxy_time + V_s_time) + " seconds\n")
	print("Collecting edges from proxies ...")

	#Send from proxy to local 
	start_time = timeit.default_timer()
	cur.execute("DROP TABLE IF EXISTS E_s_local CASCADE;")
	cur.execute("CREATE TABLE E_s_local(machine int NOT NULL,i int NOT NULL,j int NOT NULL,i_color int NOT NULL,j_color int NOT NULL);")
	cur.execute("CREATE PROJECTION E_s_local_super(machine ENCODING RLE, i, j, i_color ENCODING RLE, j_color ENCODING RLE) AS SELECT machine, i,j, i_color,j_color FROM E_s_local ORDER BY i,j SEGMENTED BY (machine*4294967295//8) ALL NODES OFFSET 0 KSAFE 1;")

	cur.execute("INSERT INTO E_s_local SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN triplet edge1 ON E.i_color=edge1.color1 and E.j_color=edge1.color2 and E.i<E.j UNION SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN triplet edge2 ON E.i_color=edge2.color2 and E.j_color=edge2.color3 and E.i<E.j UNION SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN triplet edge3 ON E.i_color=edge3.color3 AND E.j_color=edge3.color1 and E.i>E.j;")
	cur.execute("COMMIT;")

	E_s_local_time = timeit.default_timer() - start_time 
	elapsed_time += E_s_local_time

	print("Collecting edges from proxies took : " + str(E_s_local_time) + " seconds\n")

	print("Triangle enumeration ...")

	#triangles, but locally on each machine
	start_time = timeit.default_timer()
	time1 = 0
	count = 0
	cur.execute("SELECT E1.machine, E1.i AS v1, E1.j AS v2, E2.j AS v3  FROM E_s_local E1 JOIN E_s_local E2 ON E1.machine=E2.machine AND E1.j=E2.i JOIN E_s_local E3 ON E2.machine=E3.machine AND E2.j=E3.i WHERE E1.i<E1.j AND E2.i<E2.j AND E1.i=E3.j AND E1.i_color=1 AND E1.j_color=1 AND E2.j_color=1 AND local_node_name()='v_graphdb_node0001' ORDER BY v1,v2,v3;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		time1 = timeit.default_timer() - start_time
		count = cur.rowcount
		print("Execution time on machine 1: " + str(time1))
		print("Triangle count on machine 1: " + str(cur.rowcount))
		with open(outputpath+"/Randomized_"+filename, 'a+') as triangles:
			for i, row in enumerate(rows):
				triangles.write(str(row)+'\n')

	start_time = timeit.default_timer()
	cur.execute("SELECT E1.machine, E1.i AS v1, E1.j AS v2, E2.j AS v3  FROM E_s_local E1 JOIN E_s_local E2 ON E1.machine=E2.machine AND E1.j=E2.i JOIN E_s_local E3 ON E2.machine=E3.machine AND E2.j=E3.i WHERE E1.i<E1.j AND E2.i<E2.j AND E1.i=E3.j AND E1.i_color=1 AND E1.j_color=1 AND E2.j_color=2 AND local_node_name()='v_graphdb_node0002' ORDER BY v1,v2,v3;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		time2 = timeit.default_timer() - start_time
		time1 +=time2
		count += cur.rowcount
		print("Execution time on machine 2: " + str(time2))
		print("Triangle count on machine 2: " + str(cur.rowcount))
		with open(outputpath+"/Randomized_"+filename, 'a+') as triangles:
			for i, row in enumerate(rows):
				triangles.write(str(row)+'\n')

	start_time = timeit.default_timer()
	cur.execute("SELECT E1.machine, E1.i AS v1, E1.j AS v2, E2.j AS v3  FROM E_s_local E1 JOIN E_s_local E2 ON E1.machine=E2.machine AND E1.j=E2.i JOIN E_s_local E3 ON E2.machine=E3.machine AND E2.j=E3.i WHERE E1.i<E1.j AND E2.i<E2.j AND E1.i=E3.j AND E1.i_color=1 AND E1.j_color=2 AND E2.j_color=1 AND local_node_name()='v_graphdb_node0003' ORDER BY v1,v2,v3;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		time3 = timeit.default_timer() - start_time
		time1 +=time3
		count += cur.rowcount
		print("Execution time on machine 3: " + str(time3))
		print("Triangle count on machine 3: " + str(cur.rowcount))
		with open(outputpath+"/Randomized_"+filename, 'a+') as triangles:
			for i, row in enumerate(rows):
				triangles.write(str(row)+'\n')

	start_time = timeit.default_timer()
	cur.execute("SELECT E1.machine, E1.i AS v1, E1.j AS v2, E2.j AS v3  FROM E_s_local E1 JOIN E_s_local E2 ON E1.machine=E2.machine AND E1.j=E2.i JOIN E_s_local E3 ON E2.machine=E3.machine AND E2.j=E3.i WHERE E1.i<E1.j AND E2.i<E2.j AND E1.i=E3.j AND E1.i_color=1 AND E1.j_color=2 AND E2.j_color=2 AND local_node_name()='v_graphdb_node0004' ORDER BY v1,v2,v3;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		time4 = timeit.default_timer() - start_time
		time1 +=time4
		count += cur.rowcount
		print("Execution time on machine 4: " + str(time4))
		print("Triangle count on machine 4: " + str(cur.rowcount))
		with open(outputpath+"/Randomized_"+filename, 'a+') as triangles:
			for i, row in enumerate(rows):
				triangles.write(str(row)+'\n')

	start_time = timeit.default_timer()
	cur.execute("SELECT E1.machine, E1.i AS v1, E1.j AS v2, E2.j AS v3  FROM E_s_local E1 JOIN E_s_local E2 ON E1.machine=E2.machine AND E1.j=E2.i JOIN E_s_local E3 ON E2.machine=E3.machine AND E2.j=E3.i WHERE E1.i<E1.j AND E2.i<E2.j AND E1.i=E3.j AND E1.i_color=2 AND E1.j_color=1 AND E2.j_color=1 AND local_node_name()='v_graphdb_node0005' ORDER BY v1,v2,v3;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		time5 = timeit.default_timer() - start_time
		time1 +=time5
		count += cur.rowcount
		print("Execution time on machine 5: " + str(time5))
		print("Triangle count on machine 5: " + str(cur.rowcount))
		with open(outputpath+"/Randomized_"+filename, 'a+') as triangles:
			for i, row in enumerate(rows):
				triangles.write(str(row)+'\n')

	start_time = timeit.default_timer()
	cur.execute("SELECT E1.machine, E1.i AS v1, E1.j AS v2, E2.j AS v3  FROM E_s_local E1 JOIN E_s_local E2 ON E1.machine=E2.machine AND E1.j=E2.i JOIN E_s_local E3 ON E2.machine=E3.machine AND E2.j=E3.i WHERE E1.i<E1.j AND E2.i<E2.j AND E1.i=E3.j AND E1.i_color=2 AND E1.j_color=1 AND E2.j_color=2 AND local_node_name()='v_graphdb_node0006' ORDER BY v1,v2,v3;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		time6 = timeit.default_timer() - start_time
		time1 +=time6
		count += cur.rowcount
		print("Execution time on machine 6: " + str(time6))
		print("Triangle count on machine 6: " + str(cur.rowcount))
		with open(outputpath+"/Randomized_"+filename, 'a+') as triangles:
			for i, row in enumerate(rows):
				triangles.write(str(row)+'\n')

	start_time = timeit.default_timer()
	cur.execute("SELECT E1.machine, E1.i AS v1, E1.j AS v2, E2.j AS v3  FROM E_s_local E1 JOIN E_s_local E2 ON E1.machine=E2.machine AND E1.j=E2.i JOIN E_s_local E3 ON E2.machine=E3.machine AND E2.j=E3.i WHERE E1.i<E1.j AND E2.i<E2.j AND E1.i=E3.j AND E1.i_color=2 AND E1.j_color=2 AND E2.j_color=1 AND local_node_name()='v_graphdb_node0007' ORDER BY v1,v2,v3;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		time7 = timeit.default_timer() - start_time
		time1 +=time7
		count += cur.rowcount
		print("Execution time on machine 7: " + str(time7))
		print("Triangle count on machine 7: " + str(cur.rowcount))
		with open(outputpath+"/Randomized_"+filename, 'a+') as triangles:
			for i, row in enumerate(rows):
				triangles.write(str(row)+'\n')

	start_time = timeit.default_timer()
	cur.execute("SELECT E1.machine, E1.i AS v1, E1.j AS v2, E2.j AS v3  FROM E_s_local E1 JOIN E_s_local E2 ON E1.machine=E2.machine AND E1.j=E2.i JOIN E_s_local E3 ON E2.machine=E3.machine AND E2.j=E3.i WHERE E1.i<E1.j AND E2.i<E2.j AND E1.i=E3.j AND E1.i_color=2 AND E1.j_color=2 AND E2.j_color=2 AND local_node_name()='v_graphdb_node0008' ORDER BY v1,v2,v3;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		time8 = timeit.default_timer() - start_time
		time1 +=time8
		count += cur.rowcount
		print("Execution time on machine 8: " + str(time8))
		print("Triangle count on machine 8: " + str(cur.rowcount))
		with open(outputpath+"/Randomized_"+filename, 'a+') as triangles:
			for i, row in enumerate(rows):
				triangles.write(str(row)+'\n')

	time1 = time1/8
	elapsed_time = elapsed_time + time1
	print("Triangle enumeration on all machine took: " + str(time1) + " seconds")
	print("Total time is: " + str(elapsed_time) + " seconds")
	print("Total Triangle count is:" + str(count))
