import numpy as np  
#import matplotlib.pyplot as plt  
import cvxopt as opt  
from cvxopt import blas, solvers  

import funciones as Fu
import pandas as pd
file_optimo = '../../data/optimo_FM_banchile.json'
file_instrumentos = '../../data/instrumentos.json'
#file_instrumentos = 'C:\\Users\\Alejandro\\Documents\\bt\\codigo\\dev4\\comparador\\funciones\\data\\instrumentos.json'

# Turn off progress printing  
solvers.options['show_progress'] = False  

def optimal_portfolio(returns): 
    #returns=np_precio
    n = len(returns)  
    returns = np.asmatrix(returns)  
    N = 10000  
    mus = [10**(5.0 * t/N - 1.0) for t in range(N)]  
    # Convert to cvxopt matrices  
    S = opt.matrix(np.cov(returns))  
    pbar = opt.matrix(np.mean(returns, axis=1))  
    # Create constraint matrices  
    G = -opt.matrix(np.eye(n))   # negative n x n identity matrix  
    h = opt.matrix(0.0, (n ,1))  
    A = opt.matrix(4.0, (1, n))  
    b = opt.matrix(1.0)  
    # Calculate efficient frontier weights using quadratic programming  
    portfolios = [solvers.qp(mu*S, -pbar, G, h, A, b)['x']  for mu in mus]  
#    for x in portfolios:
#        np.asarray(portfolios[50])
    ## CALCULATE RISKS AND RETURNS FOR FRONTIER  
    returns = [blas.dot(pbar, x) for x in portfolios]  
    risks = [np.sqrt(blas.dot(x, S*x)) for x in portfolios]     
    ## CALCULATE THE 2ND DEGREE POLYNOMIAL OF THE FRONTIER CURVE  
#    m1 = np.polyfit(returns, risks, 2)  
#    x1 = np.sqrt(m1[2] / m1[0])  
#    # CALCULATE THE OPTIMAL PORTFOLIO  
#    wt = solvers.qp(opt.matrix(x1 * S), -pbar, G, h, A, b)['x']  
    return portfolios, returns, risks


def frontera(data1):
     #data1={'RUN':['HABITAT-A','HABITAT-B','HABITAT-C','HABITAT-D','HABITAT-E'],'Serie':['Obl','Obl','Obl','Obl','Obl']}
    #data1=js_inst
    ## NUMBER OF ASSETS  #
    
    df_fondos=pd.read_json(data1, orient='records', date_unit='s')
    
    lista1=df_fondos.values.tolist()
    filenames12 = Fu.get_filenames(lista1, 36)
    df_precio=pd.DataFrame()

    for fname in filenames12:
        df=Fu.get_df(fname)
        df['por']=100
        df1=df#[['RUN','Serie','valor_cuota','Fecha','uf','tipo_cambio','tac_total']]
        df1=pd.merge(df, df_fondos, on=['RUN', 'Serie'], how='inner')
        df_precio=df_precio.append(df1)
    
    fecha_max=df_precio.Fecha.max()
    df_precio_hoy=df_precio[(df_precio['Fecha']==fecha_max)]
    df_precio_hoy=df_precio_hoy[['RUN','Serie']]
    df_precio=pd.merge(df_precio_hoy,df_precio, on=['RUN','Serie'], how='inner')
    df_assets=df_precio[['RUN','Serie']]
    df_assets=df_assets.drop_duplicates()
    df_precio=df_precio.sort_values(by=['RUN', 'Serie','Fecha'])
    df_precio=df_precio.reset_index(drop=True)
    df_P=pd.DataFrame()
    ### calculo rentabilidad df_P
    for index, row in df_assets.iterrows():

        df_precio1=df_precio[(df_precio['RUN']==row['RUN']) & (df_precio['Serie']==row['Serie'])]
        df_lista=pd.DataFrame({'RUN':df_precio1['RUN'].max(),'Serie':df_precio1['Serie'].max()
                            ,'por':[100],'NombreFondo':df_precio1['RUN'].max()})
        
        df=Fu.get_results(df_precio1, df_lista, 100, 36,100)
        df=df['df_renta']
        df=pd.DataFrame(df,columns = ['fecha' , 'renta'])
        df['RUN']=df_precio1['RUN'].max()
        df['Serie']=df_precio1['Serie'].max()
        
        col=df_precio1['RUN'].max()+'-'+df_precio1['Serie'].max()
        
        df_P=df_P.append(df)
         
    df_precio=df_P.copy()
    df_precio=df_precio.sort_values(by=['RUN', 'Serie','fecha'])
    df_precio=df_precio.reset_index(drop=True)
    df_precio_salida=df_precio.copy()
    df_P=pd.DataFrame()
    df_P['fecha']=df_precio['fecha'].unique()
    ### matris de rentabilidad
    for index, row in df_assets.iterrows():

        df_precio1=df_precio[(df_precio['RUN']==row['RUN']) & (df_precio['Serie']==row['Serie'])]
        col=df_precio1['RUN'].max()+'-'+df_precio1['Serie'].max()
        df_precio1[col]=df_precio1['renta']
        df_precio1=df_precio1[[col,'fecha']]
        df_P=pd.merge(df_P,df_precio1, on=['fecha'], how='left')
    df_P=df_P.fillna(0)
    df_P.drop('fecha', inplace=True, axis=1)
    df_precio_t=df_P.transpose()        
    np_precio=df_precio_t.to_numpy()

    ## NUMBER OF OBSERVATIONS  
   
    portfolios, returns, risks = optimal_portfolio(np_precio)
    return portfolios, returns, risks,df_precio_salida

def prtafolio_ef(Administradora,distribuidor,Tipo):
        
   
    
    df_instrumentos=pd.read_json(file_instrumentos, orient='records', date_unit='s')
    df_instrumentos=df_instrumentos[(df_instrumentos['Administradora'].str.contains(Administradora))]
    if distribuidor!='':
        df_instrumentos=df_instrumentos[(df_instrumentos['Nombre'].str.contains(distribuidor))]
    df_instrumentos=df_instrumentos[(df_instrumentos['Tipo']==Tipo)]
    df_inst=df_instrumentos.groupby(['RUN']).agg({'Serie':'min'})
    df_inst=df_inst.reset_index(level=[0])
    js_inst=df_inst.to_json( orient='records', date_unit='s')
    portfolios, returns, risks,df_precio_salida=frontera(js_inst)
    
    df_precio_salida1=df_precio_salida.groupby(['RUN']).agg({'Serie':'min'})
    df_precio_salida1=df_precio_salida1.reset_index(level=[0])
    maximo=risks[0]*100
    delta=0.2
    i=0
    df_=pd.DataFrame()
    df_rent=pd.DataFrame(columns=['RUN','Serie','Tipo','por','vol_des','vol_hasta','ren_max'])
    for por in portfolios:
        data=np.asarray(por)
        df_1 = pd.DataFrame({'Column1':data[:,0]})
       
        if maximo>=risks[i]*100:
            df_precio_salida1[i]=df_1['Column1'].round(2)
            df_[i]=df_precio_salida1[i]
            for index, row in df_precio_salida1.iterrows():
                if row[i]>0:
                    RUN=row['RUN']
                    Serie=row['Serie']
                    por=row[i]
                    if i!=0:
                        vol_red=round(maximo,4)
                    else:
                        vol_red=99
                    vol_red_antes=round(risks[i]*100-delta,4)
                    data=[[RUN,Serie,Tipo,int(por*100*4),vol_red_antes,vol_red,round(returns[i],3)*100]]
                    df_lista_t=pd.DataFrame(data,columns=['RUN','Serie','Tipo','por','vol_des','vol_hasta','ren_max'])
                    df_rent=df_rent.append(df_lista_t)
            maximo=risks[i]*100-delta       
                        
        i=i+1     
    
    ### elimina los descuadrados %
    df_rent_3_=df_rent.copy()
    df_rent_3_.reset_index(inplace=True)
    df_rent_3_['listo']=0
    df_rent_3_g=df_rent_3_.groupby(['vol_des'], as_index=False,).agg({'por':sum,'RUN':max})
    df_rent_3_g=df_rent_3_g[(df_rent_3_g['por']!=100)]
    df_rent_3_g['por']=100-df_rent_3_g['por']
    for index, row in df_rent_3_.iterrows():
        for index1, row1 in df_rent_3_g.iterrows():
            if row['vol_des']==row1['vol_des']:
                if row['RUN']==row1['RUN']:
                    if  row['listo']==0:
                        df_rent_3_.at[index,'por']=row1['por']+row['por']
                        df_rent_3_.at[index,'listo']=1
    df_rent=df_rent_3_[['RUN','Serie','Tipo','por','vol_des','vol_hasta','ren_max']]
                        
    return df_rent

if __name__== "__main__":
    Administradora='PRINCIPAL'
    distribuidor=''
    Administradora='BANCHILE'
    distribuidor=''
#    Administradora='SECURITY'
#    distribuidor=''
#    Administradora='LARRAINVIAL'
#    distribuidor='CONSORCIO'
    
    Tipo='APV'
    df_salida=prtafolio_ef(Administradora,distribuidor,Tipo)
    Tipo='AV'
    df_salida1=prtafolio_ef(Administradora,distribuidor,Tipo)
    df_salida2=df_salida.append(df_salida1)
    
    df_salida2.reset_index(inplace=True)
    df_salida2=df_salida2[['RUN','Serie','Tipo','por','vol_des','vol_hasta','ren_max']]
    df_instrumentos=pd.read_json(file_instrumentos, orient='records', date_unit='s')
    df_instrumentos['value']=df_instrumentos['Id']
#    
#    
    df_salida3=pd.merge(df_salida2,df_instrumentos,  on=['RUN','Serie','Tipo'], how='inner')
    df_salida3.to_json(file_optimo, orient='records', date_unit='s')
