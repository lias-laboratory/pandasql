import os
import sys
sys.path.append(os.getcwd() + "/deps")


from flask import Flask
from flask import render_template, request, redirect, url_for, session
from werkzeug.utils import secure_filename
import pandas as pd

UPLOAD_FOLDER = '/home/limosadm/files/'
ALLOWED_EXTENSIONS = {'txt', 'csv'}

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.secret_key = "SQL4Triangle"

import db_model as db


from datetime import date
from datetime import timedelta

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_json(results):
	data = []
	sub_data = {'name': "", 'y': 0}
	for k,v in results:
		sub_data['name'] = "machine_"+ k[-1]
		sub_data['y'] = v
		data.append(sub_data)
		sub_data = {'name': "", 'y': 0}
	return data  
"""
def get_viz(results,result):
	edges = []
	sub_data = {'from': 0, 'to': 0}
	for k,v in results:
		sub_data['from'] = k
		sub_data['to'] = v
		edges.append(sub_data)
		sub_data = {'from': 0, 'to': 0}
	nodes = []
	sub_data = {'id': 0, 'label': ""}
	for v in result:
		sub_data['id'] = v[0]
		sub_data['label'] = str(v[0])
		nodes.append(sub_data)
		sub_data = {'id': 0, 'label': ""}

	return nodes, edges
"""
def get_high_vertices(path):
	v_list = []
	d_list = []
	colnames= ['i','j']
	E_s = pd.read_csv(path, delimiter=' ', names=colnames, header=None, index_col=False)
	print(E_s.head(10))
	indegree = pd.DataFrame(E_s.groupby(['j']).size().reset_index(name='indegree')).rename(columns={"j":"i"}).astype('int64')
	outdegree = pd.DataFrame(E_s.groupby(['i']).size().reset_index(name='outdegree')).rename(columns={"i":"i"}).astype('int64')
	V_s = indegree.merge(outdegree, on="i", how='outer').fillna(0)
	V_s['degree'] = (V_s['indegree'] + V_s['outdegree'])
	V_s = V_s.drop(V_s.columns[[1,2]], axis=1)	
	V_s = V_s.sort_values(by='degree', ascending=False).astype('uint64')
	vertices = V_s[:15]
	for index, v in vertices.iterrows():
		v_list.append(int(v['i']))
		d_list.append(int(v['degree']))
	return v_list,d_list

	"""
	edges = E_s.to_dict('records') #E_s.T.to_dict().values()
	E_s_n = pd.DataFrame(E_s['from'].append(E_s['to']).drop_duplicates(keep='first')).rename(index=str, columns={0:'id'})
	E_s_n['label'] = E_s_n['id'].astype(str)
	E_s_n = E_s_n.reset_index(drop=True)
	nodes = E_s_n.to_dict('records')
	"""
	

def get_graph_detail(path):
	colnames= ['from','to']
	E_s = pd.read_csv(path, delimiter=' ', names=colnames, header=None)
	m = E_s.size/2
	print(m)
	E_s_n = pd.DataFrame(E_s['from'].append(E_s['to']).drop_duplicates(keep='first'))
	n = E_s_n.size
	return n,m	

@app.route("/", methods=["POST","GET"])
def home():
	piechart = []
	n=0
	m=0
	print("Hello!!")
	if request.method == "POST":
		if request.form['submit_btn'] == "Run":
			dataset= ""
			triplet = ""
			file = request.files["dataset"]
			if file.filename == '':
				flash('No selected file')
				return redirect(request.url)
			if file and allowed_file(file.filename):
				filename = secure_filename(file.filename)
				file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
				dataset = UPLOAD_FOLDER + filename

			ds_type = str(request.form.get("gtype"))
			algo = str(request.form.get("algo"))
			n,m = get_graph_detail(dataset)
			session['n'] = n
			session['m'] = m
			v_list,d_list = get_high_vertices(dataset)
			session['v_list'] = v_list 
			session['d_list'] = d_list
			if algo == "standard":
				results,running_time = db.standard_te(dataset,ds_type)

				std_piechart = get_json(results)

				session['std_piechart'] = std_piechart
				session['std_time'] = float("{:.2f}".format(running_time))
				print("time is : " + str(session['std_time']))

				return render_template("dashboard.html", std_piechart=session['std_piechart'] if 'std_piechart' in session else piechart, rdm_piechart=session['rdm_piechart'] if 'rdm_piechart' in session else piechart, std_time=session['std_time'] if 'std_time' in session else 0, rdm_time=session['rdm_time'] if 'rdm_time' in session else 0, n=session['n'] if 'n' in session else n, m=session['m'] if 'm' in session else m , v_list=session['v_list'] if 'v_list' in session else [] , d_list=session['d_list'] if 'd_list' in session else [])
			elif algo == "randomized":
				file = request.files["triplet"]
			if file.filename == '':
				flash('No selected file')
				return redirect(request.url)
			if file and allowed_file(file.filename):
				filename = secure_filename(file.filename)
				file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
				triplet = UPLOAD_FOLDER + filename

				results,running_time = db.randomized_te(dataset,triplet,ds_type)
				rdm_piechart = get_json(results)
				session['rdm_piechart'] = rdm_piechart
				session['rdm_time'] = float("{:.2f}".format(running_time))
				return render_template("dashboard.html", std_piechart=session['std_piechart'] if 'std_piechart' in session else piechart, rdm_piechart=session['rdm_piechart'] if 'rdm_piechart' in session else piechart, std_time=session['std_time'] if 'std_time' in session else 0, rdm_time=session['rdm_time'] if 'rdm_time' in session else 0, n=session['n'] if 'n' in session else n, m=session['m'] if 'm' in session else m, v_list=session['v_list'] if 'v_list' in session else [] , d_list=session['d_list'] if 'd_list' in session else [])
			else:
				return redirect(request.url)
		else:
			session.clear()
			return redirect(request.url)				
	else:
		return render_template("dashboard.html", std_piechart=session['std_piechart'] if 'std_piechart' in session else piechart, rdm_piechart=session['rdm_piechart'] if 'rdm_piechart' in session else piechart, std_time=session['std_time'] if 'std_time' in session else 0, rdm_time=session['rdm_time'] if 'rdm_time' in session else 0, n=session['n'] if 'n' in session else n, m=session['m'] if 'm' in session else m, v_list=session['v_list'] if 'v_list' in session else [] , d_list=session['d_list'] if 'd_list' in session else [])


if __name__ == "__main__":
	app.run(debug=True) 
