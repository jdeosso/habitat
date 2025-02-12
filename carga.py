# -*- coding: utf-8 -*-
from pandasticsearch import Select
import elasticsearch as es

class Carga:
    sid =''
    def __init__(self, url='', indice='', tamanno=''):
        self.url=url
        self.indice =indice
        self.tamanno =tamanno
        
        
    def carga(self,query=''): 
        self.query =query
        es1 = es.Elasticsearch([self.url])
        data=es1.search(index=self.indice, body=self.query, scroll='2m',  size = self.tamanno, request_timeout=200)
        sid = data['_scroll_id']
        scroll_size = (data['hits']['total']  )
        # define tabla final
        data1=Select.from_dict(data).to_pandas()
        # ciclo dond tare los datos
        while (scroll_size > 0):
            try:
                data = es1.scroll(scroll_id=sid, scroll='2m')
                data1=data1.append(Select.from_dict(data).to_pandas() ,ignore_index=True)
                    # Update the scroll ID
                sid = data['_scroll_id']
                # Get the number of results that we returned in the last scroll
                scroll_size = len(data['hits']['hits'])
            
            except:
                break
        
        return data1
    def inserta(self,indice,Type, query=''):
        es1 = es.Elasticsearch([self.url])
        es1.index(index=indice,    doc_type=Type, body=query)
        
       