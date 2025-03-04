import pandas as pd

df = pd.read_csv("E:\VM\Shared\ISOT_Botnet_DataSet_2010.pcap_Flow - Copy.csv")

columnToExtract = ['Src IP', 'Dst IP', 'Protocol', 'Timestamp', 'Tot Fwd Pkts', 'Tot Bwd Pkts', 'TotLen Fwd Pkts', 'TotLen Bwd Pkts', 'Fwd Seg Size Avg', 'Bwd Seg Size Avg']
df[columnToExtract].to_csv("E:\VM\Shared\ISOT_Botnet_DataSet_2010.pcap_Flow_Extracted.csv", index=False)