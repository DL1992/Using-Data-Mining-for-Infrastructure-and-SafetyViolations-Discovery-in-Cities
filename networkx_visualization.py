from collections import defaultdict
import pandas as pd
import numpy as np
from geopy import distance
from geopy.distance import geodesic
import os
import networkx as nx
import matplotlib.pyplot as plt
from operator import itemgetter
import folium
from geopy import distance
from shapely.geometry import Point, Polygon


def get_edges_to_obj(df, object_to_validate, dataset_name):
  """
      This function creates a new dataframe with the distance between given objects and a specific object (e.g. fire
      hydrants or shelters).
      the new dataframe has source id of an object, destination id of another object and the distance between the two
      objects.
      :param df: the dataframe of object to calculate the distance from fire hydrants.
      :param object_to_validate: The type of object that we want to get distance to ('fire_hydrant' or 'shelter')
      :param dataset_name: the name of the dataset - used for saving the resulting dataframe.
  """
  df = df.replace(np.nan, 'unknown', regex=True)
  if object_to_validate == 'fire_hydrant':
    validate_df = pd.read_csv("./Fire_Hydrant.csv")
  else:  # shelter
    validate_df = pd.read_csv("./shelters.csv")
  edges = pd.DataFrame(columns=['source', 'dest', 'dist'])
  for index, row in validate_df.iterrows():
    for index2, row2 in df.iterrows():
      cur_dist = distance.distance((row['Y'], row['X']), (row2['Y'], row2['X']))
      edge = pd.DataFrame([[row['Id'], row2['Id'], cur_dist]],
                          columns=['source', 'dest', 'dist'])
      edges = edges.append(edge)
  edges['dist'] = edges['dist'].apply(lambda x: float(str(x).split()[0]))
  edges = edges[edges['dist'] != 0.0]
  edges.to_csv('./edges_to_' + object_to_validate + 's/' + 'edges_' + dataset_name + '.csv', index=False)


def get_fire_hydrants_edges(start, end):
  """
      This function is part of the pre-processing step when you want to parallize the process of creating egged between all
       fire hydrant's. it save the dataframe in the disk to unit them later after the parallel work is done.
       It's is not used NOW.
      :param start: the start index of fire hydrantes df for specific process
      :param end: the end index of fire hydrantes df for specific process
  """
  df = pd.read_csv("./Fire_Hydrant.csv")
  df2 = df.iloc[start:end]
  edges = pd.DataFrame(columns=['source', 'dest', 'dist'])
  for row in df.itertuples():
    for row2 in df2.itertuples():
      cur_dist = distance.distance((row[2], row[1]), (row2[2], row2[1]))
      edge = pd.DataFrame([[row[4], row2[4], cur_dist]],
                          columns=['source', 'dest', 'dist'])
      edges = edges.append(edge)

  edges['dist'] = edges['dist'].apply(lambda x: float(str(x).split()[0]))
  edges = edges[edges['dist'] != 0.0]
  edges.to_csv("Edges/hydrants_edges" + str(start) + ".csv", index=False)


def unite_csv():
  """
      A function to unite all batch dataframes csv files created using 'get_fire_hydrants_edges' in the use
      of parallel pre-processing. saves the resulting dataframe to disk.
  """
  edges = pd.DataFrame(columns=['source', 'dest', 'dist'])
  for file in os.listdir("./"):
    if "%" in file:
      current = pd.read_csv(file)
      edges = edges.append(current)
  edges.to_csv("all_hydrants_edges.csv", index=False)


def create_graph(df=None, coordinates=None, threshold=0.1):
  """
      Main function to crate the graph of object geographical points.
      :param df: the dataframe with distance between all points - the edges of the graph.
      :param coordinates: a dictionary containing all the nodes data - the vertices of the graph.
      :param threshold: only edges smaller than the threshold are used in the graph.
      :return: the networkx graph object.
  """
  g = nx.Graph()
  for key, value in coordinates.items():
    if isinstance(value[2], str):
      g.add_node(key, x=(value[0], value[1]), name=value[2], color=value[3])
    else:
      g.add_node(key, x=(value[0], value[1]), name=value[2], color=value[3])

  df = df[df['dist'] < threshold]
  edges_list = [(r['source'], r['dest']) for i, r in df.iterrows()]
  g.add_edges_from(edges_list)
  return g


def draw_nx(g, min_x, max_x, min_y, max_y):
  """
      Function to draw networkX graph
      :param g: the graph to draw
      :param min_x: min long coordinate
      :param max_x: max long coordinate
      :param min_y: min lang coordinate
      :param max_y: max lang coordinate
      :return: plot the graph
  """
  plt.figure(figsize=(30, 30))
  color = nx.get_node_attributes(g, 'color')
  pos = nx.get_node_attributes(g, 'x')
  names = nx.get_node_attributes(g, 'name')
  nx.draw_networkx_nodes(g, pos=pos, alpha=0.7, node_color=color.values())
  nx.draw_networkx_labels(g, pos=pos, labels=names)
  nx.draw_networkx_edges(g, pos=pos, width=4)
  plt.xlim([min_x, max_x])
  plt.ylim([min_y, max_y])
  plt.axis('on')
  plt.show()


def unite_edges(objects_to_display, object_to_validate):
  """
  A function that unites all of the relevant objects' edges csv's to a single united df and returns it
  :param objects_to_display: dictionary of the different objects
  :param object_to_validate: object type to get edges from the objects to ('fire_hydrant' or 'shelter')
  :return: united df
  """
  total_df = pd.DataFrame(columns=['source', 'dest', 'dist'])
  for object in objects_to_display.keys():
    total_df = total_df.append(pd.read_csv('./edges_to_' + object_to_validate + 's/' + 'edges_' + object + '.csv'),
                               ignore_index=True)
  return total_df


def create_all_graphs(objects_to_display, object_to_validate, distance=0.03):
  """
  :param objects_to_display: list of the different object types to display.
  :param object_to_validate: object type to get edges from the objects to ('fire_hydrant' or 'shelter').
  :param distance: only edges smaller than the threshold are used in the graph.
  :return: the graph that was created
  """
  relevant_objects = {k: v for k, v in all_objects.items() if k in objects_to_display}
  coordinates = defaultdict(tuple)
  for object, color in relevant_objects.items():
    df = pd.read_csv('./Modified_datasets/' + object + '.csv')
    df = df[['X', 'Y', 'Name', 'Id']]
    for row in df.itertuples():
      coordinates[row[4]] = (row[1], row[2], row[3], color)
  if object_to_validate == "fire_hydrant":
    validate_df = pd.read_csv("./Fire_Hydrant.csv")
    validate_df['Name'] = validate_df['Id']
  else:  # shelter
    validate_df = pd.read_csv("./shelters.csv")
  validate_df = validate_df[['X', 'Y', 'Name', 'Id']]
  for row in validate_df.itertuples():
    coordinates[row[4]] = (row[1], row[2], row[3], 'red')
  g = create_graph(unite_edges(relevant_objects, object_to_validate), coordinates, distance)
  return g


def nodes_per_neighborhood(g, neighborhoods):
  """
      Return a list of all nodes from the graph that are in a specific neighborhoods.
      :param g: the complete graph containing all nodes and edegs
      :param neighborhoods: a list of neighborhoods we want to filter the graph by.
      :return: All nodes in the graph that are in the given neighborhoods.
  """
  nodes = []
  neighborhoods_coordinates = return_neighborhoods_coordinates()
  for node in g.nodes(data='x'):
    p = Point(node[1][::-1])
    for neighborhood in neighborhoods:
      if p.within(neighborhoods_coordinates[neighborhood]):
        nodes.append(node[0])
        break
  return nodes


def get_isloated_nodes(g, relevant_nodes):
  """
      Filtter from a given graph all nodes but the relevant nodes we want.
      :param g: the complete graph containing all nodes and edges
      :param relevant_nodes: list of relevant nodes to check
      :return: a list all isolated nodes (no edge to fire hydrant) from a list of relevant nodes to check.
  """
  isolated_nodes = []
  for node in g.nodes:
    if 2595 < node < 3583:
      if g.degree[node] == 0 and node in relevant_nodes:
        isolated_nodes.append(node)
  return isolated_nodes


# decide which objects to add to the map (all together is overwhelming)
all_objects = {'community-centers': 'blue',
               'daycare': 'purple',
               'gas_stations': 'cyan',
               'EducationalInstitutions': 'green',
               'HealthClinics': 'pink',
               'Sport': 'orange',
               'Synagogue': 'white'}

neighborhoods_coordinates = defaultdict(list)  # [lat_start, lat_end, long_start, long_end]
neighborhoods_coordinates['Ramot'] = Polygon(
  [(31.286587, 34.791335), (31.274997, 34.795884), (31.270082, 34.800004), (31.263772, 34.806699),
   (31.267881, 34.817428), (31.272943, 34.828071), (31.280719, 34.825324), (31.280645, 34.821290),
   (31.289374, 34.806184)])
neighborhoods_coordinates['Dalet'] = Polygon(
  [(31.271476, 34.781207), (31.264653, 34.789018), (31.259003, 34.787473), (31.258196, 34.797858),
   (31.255702, 34.798459), (31.256215, 34.810990), (31.257389, 34.812020), (31.274924, 34.796571)])
neighborhoods_coordinates['Vav'] = Polygon(
  [(31.271541, 34.780349), (31.266479, 34.768676), (31.260609, 34.773311), (31.258335, 34.775800),
   (31.258995, 34.787387), (31.263984, 34.789361), (31.270661, 34.781207)])
neighborhoods_coordinates['Bet'] = Polygon(
  [(31.258449, 34.797977), (31.258522, 34.775833), (31.251625, 34.786562), (31.250671, 34.797548),
   (31.257715, 34.797977)])
neighborhoods_coordinates['Gimel'] = Polygon(
  [(31.255238, 34.798757), (31.250762, 34.798070), (31.244891, 34.804765), (31.251642, 34.815751),
   (31.256558, 34.812404)])
neighborhoods_coordinates['Down Town'] = Polygon(
  [(31.250392, 34.797997), (31.245916, 34.796710), (31.243935, 34.793105), (31.239311, 34.797825),
   (31.244522, 34.804434)])
neighborhoods_coordinates['Alef'] = Polygon(
  [(31.243861, 34.793191), (31.248117, 34.797911), (31.250906, 34.797825), (31.252153, 34.786496),
   (31.246356, 34.782719)])
neighborhoods_coordinates['Hei'] = Polygon(
  [(31.246429, 34.782290), (31.252080, 34.770188), (31.258390, 34.775595), (31.252153, 34.786067)])
neighborhoods_coordinates['Tet'] = Polygon(
  [(31.252226, 34.770274), (31.241293, 34.767441), (31.244595, 34.782633), (31.246209, 34.782977),
   (31.251566, 34.771733)])
neighborhoods_coordinates['Yod Alef'] = Polygon(
  [(31.266313, 34.768557), (31.256849, 34.758429), (31.251933, 34.770102), (31.258610, 34.775595)])
neighborhoods_coordinates['Old Town'] = Polygon(
  [(31.239091, 34.798169), (31.233367, 34.784178), (31.235128, 34.770789), (31.241293, 34.767527),
   (31.244375, 34.782033), (31.246283, 34.782719)])
neighborhoods_coordinates['Ashan'] = Polygon(
  [(31.266844, 34.768721), (31.272053, 34.762885), (31.262222, 34.749495), (31.257012, 34.758593),
   (31.265890, 34.767606)])
neighborhoods_coordinates['Noi Beka'] = Polygon(
  [(31.239255, 34.798076), (31.230301, 34.802110), (31.225971, 34.797732), (31.218337, 34.774043),
   (31.221861, 34.768550), (31.234044, 34.774472), (31.238447, 34.794986)])
neighborhoods_coordinates['Darom'] = Polygon(
  [(31.230958, 34.803023), (31.225967, 34.797272), (31.220095, 34.799590), (31.220169, 34.804654),
   (31.212021, 34.802336), (31.212681, 34.824738), (31.225380, 34.826026)])
neighborhoods_coordinates['Nahot'] = Polygon(
  [(31.256796, 34.758152), (31.249751, 34.755148), (31.241239, 34.758667), (31.224359, 34.762959),
   (31.226047, 34.771799), (31.234634, 34.776005), (31.237276, 34.768881), (31.242340, 34.767508),
   (31.251953, 34.769997)])


def return_neighborhoods_coordinates():
  """
      return the neighborhoods_coordinates dict to be used in other modules/ functions
      :return: neighborhoods_coordinates dict
  """
  return neighborhoods_coordinates


template = """
  {% macro html(this, kwargs) %}

  <!doctype html>
  <html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>jQuery UI Draggable - Default functionality</title>
    <link rel="stylesheet" href="//code.jquery.com/ui/1.12.1/themes/base/jquery-ui.css">

    <script src="https://code.jquery.com/jquery-1.12.4.js"></script>
    <script src="https://code.jquery.com/ui/1.12.1/jquery-ui.js"></script>

    <script>
    $( function() {
      $( "#maplegend" ).draggable({
                      start: function (event, ui) {
                          $(this).css({
                              right: "auto",
                              top: "auto",
                              bottom: "auto"
                          });
                      }
                  });
  });
    </script>
  </head>
  <body>
  <div id='maplegend' class='maplegend' 
      style='position: absolute; z-index:9999; border:2px solid grey; background-color:rgba(255, 255, 255, 0.8);
       border-radius:6px; padding: 10px; font-size:14px; right: 20px; bottom: 20px;'>

  <div class='legend-title'>Legend (draggable!)</div>
  <div class='legend-scale'>
    <ul class='legend-labels'>
      <li><span style='background:blue;opacity:0.7;'></span>Community centers</li>
      <li><span style='background:purple;opacity:0.7;'></span>Daycare</li>
      <li><span style='background:cyan;opacity:0.7;'></span>Gas stations</li>
      <li><span style='background:green;opacity:0.7;'></span>Educational institutions</li>
      <li><span style='background:pink;opacity:0.7;'></span>Health clinics</li>
      <li><span style='background:orange;opacity:0.7;'></span>Sport</li>
      <li><span style='background:white;opacity:0.7;'></span>Synagogue</li>
    </ul>
  </div>
  </div>
  </body>
  </html>
  <style type='text/css'>
    .maplegend .legend-title {
      text-align: left;
      margin-bottom: 5px;
      font-weight: bold;
      font-size: 90%;
      }
    .maplegend .legend-scale ul {
      margin: 0;
      margin-bottom: 5px;
      padding: 0;
      float: left;
      list-style: none;
      }
    .maplegend .legend-scale ul li {
      font-size: 80%;
      list-style: none;
      margin-left: 0;
      line-height: 18px;
      margin-bottom: 2px;
      }
    .maplegend ul.legend-labels li span {
      display: block;
      float: left;
      height: 16px;
      width: 30px;
      margin-right: 5px;
      margin-left: 0;
      border: 1px solid #999;
      }
    .maplegend .legend-source {
      font-size: 80%;
      color: #777;
      clear: both;
      }
    .maplegend a {
      color: #777;
      }
  </style>
  {% endmacro %}"""

def return_template_string():
  return template
