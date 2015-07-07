import storm
import re
import whois
import requests
from bs4 import BeautifulSoup
from goose import Goose

class Crawler(storm.BasicBolt):
    def process(self,tup):
        if tup.values:
            url=tup.values[0]
            linkPattern = re.compile("^(?:ftp|http|https):\/\/(?:[\w\.\-\+]+:{0,1}[\w\.\-\+]*@)?(?:[a-z0-9\-\.]+)(?::[0-9]+)?(?:\/|\/(?:[\w#!:\.\?\+=&amp;%@!\-\/\(\)]+)|\?(?:[\w#!:\.\?\+=&amp;%@!\-\/\(\)]+))?$")
            unwanted=re.compile('.*join.*|.*project.*|.*javascript.*|.*blog.*|.*mailto.*|.*pdf.*|.*recruit.*|.*events?.*|.*facts.*|.*mission.*|.*values.*|.*faq.*|.*news.?r?.*|.*career.*|.*updates.*|.*vision.*|.*award.*|.*products.*|.*polic(y|ies).*|.*capabilities.*|.*feedback.*|.*support.*|.*innovaitons.*',re.IGNORECASE)
            lang=re.compile('.*japanese.*|.*mandarin.*|.*portuguese.*|.*germen.*|.*french.*|.*twitter.*|.*linkedin.*|.*google.*|.*youtube.*|.*facebook.*',re.IGNORECASE)
            err=re.compile('.*runtime.?error.*|.*403.?.?forbidden.*|.*not.?found.*',re.IGNORECASE)
            about=re.compile('.*about.*|.*Company Overview.*|.*who.we.are.*|.*what.we.do.*',re.IGNORECASE)
            company=re.compile('.*company(.?overview)?.*|.*introduction.*',re.IGNORECASE)
            management=re.compile('.*management.*|.*directors.*|.*team.*|.*exec.*|.*bod.*|.*leadership.*|.*staff.*|.*board.*',re.IGNORECASE)
            contact=re.compile('.*contact.*',re.IGNORECASE)
            desc_link=[]
            crawledLink=[]
            boo=True

            filname=url.replace('http://','')
            filname=filname.replace( 'https://','')
            filname=filname.replace('www.','')
            filname=re.sub('\..*|\/.*','',filname)
            def crawl(link1):
                try:
                    global depth_level
                    depth_level=depth_level+1
                    if depth_level<=10:
                        html=requests.get(link1)
                        soup1=BeautifulSoup(html.content)
                        for l in soup1.find_all('a'):
                            l1=str(l.get('href'))
                            #p l1
                            if not linkPattern.match(l1):
                                if l1[0]!='/':
                                    l1=url+'/'+l1
                                else:
                                    l1=url+l1

                            if (about.match(l1) or about.match(l.get_text())) and  not ( unwanted.match(l1) or unwanted.match(l.get_text())) and not lang.match(l1) and not l1 in crawledLink:
                                crawledLink.append(l1)
                                #p l1
                                if not l1 in desc_link:
                                    desc_link.append(l1)
                                crawl(l1)

                except:
                    pass #p "ERROR1 with " + link1


            try:

                shtml=requests.get(url)
                desc_link=[]
                soup = BeautifulSoup(shtml.content)

                for link in soup.find_all('a'):
                    link1=str(link.get('href'))
                    #p link1
                    if (about.match(link1) or about.match(link.get_text())) and not (unwanted.match(link1) or unwanted.match(link.get_text())) and not lang.match(link1):
                        #p "tttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt"
                        if not linkPattern.match(link1) :
                            if link1[0]!='/':
                                link1=url+'/'+link1
                            else:
                                link1=url+link1
                        if not link1 in crawledLink:
                            crawledLink.append(link1)
                            depth_level=0
                            desc_link.append(link1)
                            crawl(link1)
            except:
                pass

            if len(desc_link)==0:
                try:
                    shtml=requests.get(url)
                    desc_link=[]
                    soup = BeautifulSoup(shtml.content)
                    for link in soup.find_all('a'):
                        link1=str(link.get('href'))
                        #p link1
                        if (company.match(link1)) and not lang.match(link1) and not unwanted.match(link1):
                            #p "tttwtttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt"
                            if not linkPattern.match(link1) :
                                if link1[0]!='/':
                                    link1=url+'/'+link1
                                else:
                                    link1=url+link1
                            if not link1 in crawledLink:
                                crawledLink.append(link1)
                                depth_level=0
                                desc_link.append(link1)
                                crawl(link1)
                                #p desc_link
                except:
                    pass#p "ERROR in website: " + url
            g=Goose()
            about1={'about':''}

            if len(desc_link)==0:
                desc_link.append(url)
            art=g.extract(url=url)
            meta={}
            meta['description']=art.meta_description
            meta['keywords']=art.meta_keywords
            about1['meta_data']=meta
            address={}
            try:
                d=whois.whois(url)

                address['city']=d.items()[4][1]
                address['state']=d.items()[10][1]
                address['country']=d.items()[8][1]
                address['zipcode']=d.items()[6][1]
                #address['street']=d.items()[13][1]
                about1['address']=address
                about1['name']=d.items()[15][1]
                nm=re.compile('.*registrant.street.*|.*domains.by.proxy.*',re.IGNORECASE)
                if nm.match(str(about1['name'])):
                    about1['name']=art.title

            except:
                pass#p "domain name ERROR"

            for link in desc_link:
                try:
                    if not boo:
                        break
                    text=[]
                    html = requests.get(link)
                    raw = BeautifulSoup(html.content)
                    if err.match(str(raw('title'))) or err.match(str(raw('text'))):
                        #p "server error with "+link
                        continue
                    if not management.match(link) and not contact.match(link) and not unwanted.match(link)  and boo:
                        #p link

                        art=g.extract(url=link)

                        soup=BeautifulSoup(requests.get(link).content)
                        for s in soup('style'):
                            s.extract()
                        for s in soup('script'):
                            s.extract()
                        for s in soup('input'):
                            s.extract()

                        for s in soup('li'):
                            if s('a') or s('link'):
                                s.extract()
                        for s in soup('a'):
                            s.extract()


                        licount=len(soup.find_all('li'))
                        pcount=len(soup.find_all('p'))
                        tdcount=len(soup.find_all('td'))
                        if pcount>licount and pcount>tdcount:
                            for p in soup.find_all(['p','li','article']):
                                if len(p.get_text())>100 and p.name in ['p','article']:
                                    text.append(re.sub(r"\n*|\t*|\r*",'',p.get_text()))
                                elif p.name in ['li']:
                                    text.append(re.sub(r"\n*|\t*|\r*",'',p.get_text()))
                        text1=' '.join(text)
                        text2=re.sub(r"\n*|\t*|\r*",'',art.cleaned_text)
                        if len(text1) > len(text2):
                            about1['about']=text1
                        else:
                            about1['about']=text2
                        boo=False

                        if not about1['about'] or len(about1['about'])<10:
                            boo=True
                except:
                    return {'error':"ERROR2 with " + link}
                    #try:
                    #    if about1:
                    #json.dump(about1,outfile)
            about1['filename']=filname
            d=about1

            #except:
            #    pass





            #d=crawler.getabout(url)
            storm.emit([d])
            #storm.ack(tup)
            #print about
        else:
            return #"Unable to process"
            #pass
Crawler().run()