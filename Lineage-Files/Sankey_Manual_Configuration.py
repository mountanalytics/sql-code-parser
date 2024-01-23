import pandas as pd
import plotly.graph_objects as go

df = pd.read_csv('C:/Users/JurriaanGroot/OneDrive - Mount Analytics/Bureaublad/NN/Lineage/SQL_LINEAGE.csv', sep = ';')
df_labels = pd.read_csv('C:/Users/JurriaanGroot/OneDrive - Mount Analytics/Bureaublad/NN/Lineage/SQL_LINEAGE_LABELS.csv', sep = ';')

df['SOURCE_ID'].values.tolist()
df['TARGET_ID'].values.tolist()
df['LINK_VALUE'].values.tolist()
df['RAT_SCORE'].values.tolist()
df['SOURCE_FIELD'].values.tolist()
df['TARGET_FIELD'].values.tolist()
df['TRANSFORMATION'].values.tolist()
df['COLOR'].values.tolist()
df_labels['TableLabel'].values.tolist()

df['source_to_target'] = df[['SOURCE_FIELD', 'TARGET_FIELD']].agg('=>'.join, axis=1)
# df['source_to_target_transformation'] = df[['source_to_target', 'TRANSFORMATION']].agg('-'.join, axis=1)
df['source_to_target_transformation'] = df[['source_to_target','TRANSFORMATION']].apply(lambda x : '{}<br /> Transformation: {}'.format(x[0],x[1]), axis=1)

fig = go.Figure(data=[go.Sankey(
    node = dict(
      pad = 20,
      thickness = 20,
      line = dict(color = "black", width = 0.5),
      #label = ["tableA", "tableB", "tableC", "tableD","tableE","tableF"],
      label = df_labels['TableLabel'],
      #groups = [[0, 2, 4, 7]],
      color = ['aliceblue','aliceblue','lightyellow','aliceblue','aliceblue','lightyellow']
     
      #hovertemplate='Node %{customdata} has total value %{value}<extra></extra>',
      #color = "blue"

    ),
    link = dict(
      arrowlen=15,
      line = dict(color = "blue", width = 0.05),
      hoverlabel = dict (font = dict(size=15) ),
      source = df['SOURCE_ID'], # indices correspond to labels, eg A1, A2, A2, B1, ...
      target = df['TARGET_ID'],
      value = df['LINK_VALUE'],
      customdata = df['source_to_target_transformation'],
      hovertemplate='Details: %{customdata}',
      color = df['COLOR'],
      #source = [0, 1, 0, 2, 3, 3],
      #target = [2, 3, 3, 4, 4, 5],
      #value = [8, 4, 2, 8, 4, 2],
      #customdata = ["q","r","s","t","u","v"],
      #hovertemplate='Link from node %{source.customdata}<br />'+'to node%{target.customdata}<br />has value %{value}'+ '<br />and data %{customdata}<extra></extra>',
  ),
  #orientation= "v"
  #visible = "legendonly"
 
  )])

fig.update_layout(title_text="Basic Sankey Diagram", font_size=10)
fig.show()