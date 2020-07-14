from networkx_visualization import *

def visualize_fire_hydrants_network(distance=0.03):
  """
    This function draw the fire hydrants network using networkx and create_graph function.
    :param distance: Threshold for creating edge between two points. default 0.3 as per regulations
    :return: the graph object being drawn
  """
  coordinates = defaultdict(tuple)
  df_fire = pd.read_csv("./Fire_Hydrant.csv")
  df_fire['Name'] = df_fire['Id']
  df_fire = df_fire[['X', 'Y', 'Name', 'Id']]
  for row in df_fire.itertuples():
    coordinates[row[4]] = (row[1], row[2], row[3], 'red')
  min_x = df_fire['X'].min()
  max_x = df_fire['X'].max()
  min_y = df_fire['Y'].min()
  max_y = df_fire['Y'].max()
  fire_hydrants_edges = pd.read_csv("./all_hydrants_edges.csv")
  g = create_graph(fire_hydrants_edges, coordinates, distance)
  draw_nx(g, min_x, max_x, min_y, max_y)
  return g

def display_isolated_objects(objects, object_to_validate, threshold, neighborhoods):
  """
  Displays objects that are not within a given threshold to any fire hydrant
  :param objects: the types of object to present
  :param object_to_validate: The type of object that we want to get distance to ('fire_hydrant' or 'shelter')
  :param threshold: the distance threshold to a fire hydrant
  :param neighborhoods: the neighborhoods to present the data in
  :return:
  """

  g = create_all_graphs(objects, object_to_validate, threshold)
  relevant_nodes = nodes_per_neighborhood(g, neighborhoods)
  isolated_nodes = get_isloated_nodes(g, relevant_nodes)
  m = folium.Map(location=[31.2530, 34.7915], tiles='Stamen Terrain',
                 zoom_start=13, control_scale=True, prefer_canvas=True)
  colors = nx.get_node_attributes(g, 'color')
  poses = nx.get_node_attributes(g,'x')
  names = nx.get_node_attributes(g, 'name')
  for node in g.nodes:
    if node in isolated_nodes:
      icon = folium.Icon(**{'prefix': 'fa', 'color': colors[node], 'icon': 'arrow-up'})
      folium.Marker([poses[node][1], poses[node][0]], popup=str(names[node]), icon=icon).add_to(m)
  return m

if __name__ == "__main__":
  # m = display_isolated_objects(['community-centers'], 0.01, ['Alef', 'Dalet', 'Vav'])

  # for dataset in os.listdir(['community-centers.csv', 'Sport.csv']):
  for dataset in ['daycare.csv', 'Synagogue.csv']:

    print(dataset)
    df = pd.read_csv('./Modified_datasets/' + dataset)
    get_edges_to_obj(df, 'shelter', dataset.split('.')[0])
