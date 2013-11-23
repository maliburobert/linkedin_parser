# _*_ coding: utf_8 _*_
import os, csv, datetime, time, threading, Queue
from lxml import etree
count = 0
queue = Queue.Queue()
out_queue = Queue.Queue()


class ThreadUrl(threading.Thread):
    """Threaded Url Grab"""


    def __init__(self, queue, out_queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.out_queue = out_queue

    def chars(self, element):
        element = '|'.join(element)
        element = unicode(element).encode('cp1252', 'replace')
        return [element]
    
    def striplb(self, element):
        try:
            element = element[0].replace('\r\n','')
            element = element.replace('  ','')
            element = element.replace('\t','')
            element = element.replace('| |','|')
            element = element.replace('|||','|')
            element = element.replace('||','|')
            element = element.replace(' |','|')
            element = unicode(element).encode('cp1252', 'replace')
            return [element]
        except Exception:
            return [element]
            
    def run(self):
        while True:
            global count
            parser = etree.HTMLParser(encoding="UTF-8")
            getall = self.queue.get()
            universal_path = getall[0]
            file = getall[1]
            id = [file.split('.')[0]]
            count += 1
            if count % 5000 == 0: 
                print str(count) + ' ' + universal_path + ' ' + str(time.time() - start)
            if count >= 0: #this is how I resume
                try:
                    tree = etree.parse(universal_path, parser)
                    title_current_1 = self.chars(tree.xpath('//*[@class="position  first experience vevent vcard summary-current"]//*[@class="title"]//text()'))
                    company_current_1 = self.chars(tree.xpath('//*[@class="position  first experience vevent vcard summary-current"]//*[@class="org summary"]//text()'))
                    org_stats_current_1 = self.striplb(self.chars(tree.xpath('//*[@class="position  first experience vevent vcard summary-current"]//*[@class="orgstats organization-details current-position"]//text()'))) #linebreaks
                    date_start_current_1 = self.chars(tree.xpath('//*[@class="position  first experience vevent vcard summary-current"]//*[@class="dtstart"]//text()'))
                    date_end_current_1 = self.chars(tree.xpath('//*[@class="position  first experience vevent vcard summary-current"]//*[@class="dtstamp"]//text()'))
                    duration_current_1 = tree.xpath('//*[@class="position  first experience vevent vcard summary-current"]//*[@class="duration"]//text()')
                    try:
                        duration_current_1 = [duration_current_1[1]]
                    except Exception:
                        duration_current_1 = ['']
                    location_current_1 = self.chars(tree.xpath('//*[@class="position  first experience vevent vcard summary-current"]//*[@class="location"]//text()'))
                    desc_current_1 = self.striplb(self.chars(tree.xpath('//*[@class="position  first experience vevent vcard summary-current"]//*[@class=" description current-position"]//text()'))) #linebreaks
                    connections = tree.xpath('//*[@class="overview-connections"]//text()')
                    try:
                        connections = [connections[2]]
                    except Exception:
                        connections = ['']
                    first_name = self.chars(tree.xpath('//*[@class="profile-header"]//*[@class="given-name"]//text()'))
                    last_name = self.chars(tree.xpath('//*[@class="profile-header"]//*[@class="family-name"]//text()'))
                    title_comp_top = self.striplb(self.chars(tree.xpath('//*[@class="profile-header"]//*[@class="headline-title title"]//text()'))) #linebreaks
                    locality = self.striplb(self.chars(tree.xpath('//*[@class="profile-header"]//*[@class="locality"]//text()'))) #linebreaks
                    industry = self.striplb(self.chars(tree.xpath('//*[@class="profile-header"]//*[@class="industry"]//text()'))) #linebreaks
                    summary = self.striplb(self.chars(tree.xpath('//*[@class="content"]//*[@class=" description summary"]//text()')))
                    specialties = self.striplb(self.chars(tree.xpath('//*[@class="content"]//*[@class="null"]//text()')))
                    add_info_assoc = self.striplb(self.chars(tree.xpath('//*[@class="pubgroups"]//*[@class="null"]//text()')))
                    interests = self.striplb(self.chars(tree.xpath('//*[@class="content"]//*[@class="interests"]//text()')))
                    honors = self.striplb(self.chars(tree.xpath('//*[@class="honors"]//*[@class=" ''"]//text()')))
                    edu_school_1 = self.striplb(self.chars(tree.xpath('//*[@class="position  first education vevent vcard"]//*[@class="summary fn org"]//text()')))
                    edu_degree_1 = self.striplb(self.chars(tree.xpath('//*[@class="position  first education vevent vcard"]//*[@class="degree"]//text()')))
                    edu_major_1 = self.striplb(self.chars(tree.xpath('//*[@class="position  first education vevent vcard"]//*[@class="major"]//text()')))
                    edu_start_1 = self.striplb(self.chars(tree.xpath('//*[@class="position  first education vevent vcard"]//*[@class="dtstart"]//text()')))
                    edu_end_1 = self.striplb(self.chars(tree.xpath('//*[@class="position  first education vevent vcard"]//*[@class="dtend"]//text()')))
                    edu_desc_1 = self.striplb(self.chars(tree.xpath('//*[@class="position  first education vevent vcard"]//*[@class=" desc details-education"]//text()')))
                    vol_title_1 = self.striplb(self.chars(tree.xpath('//*[@class="volunteering experienced"]//*[@class="experience  first"]//*[@class="title"]//text()')))
                    vol_org_1 = self.striplb(self.chars(tree.xpath('//*[@class="volunteering experienced"]//*[@class="experience  first"]//*[@class="postitle"]/h5/strong/span//text()')))
                    vol_specifics_1 = self.striplb(self.chars(tree.xpath('//*[@class="volunteering experienced"]//*[@class="experience  first"]//*[@class="postitle"]//*[@class="specifics"]//text()')))
                    vol_start_1 = self.striplb(self.chars(tree.xpath('//*[@class="volunteering experienced"]//*[@class="experience  first"]//*[@class="period"]/abbr[1]//text()')))
                    vol_stop_1 = self.striplb(self.chars(tree.xpath('//*[@class="volunteering experienced"]//*[@class="experience  first"]//*[@class="period"]/abbr[3]//text()')))
                    vol_duration_1 = self.striplb(self.chars(tree.xpath('//*[@class="volunteering experienced"]//*[@class="experience  first"]//*[@class="period"]//*[@class="duration"]//text()')))
                    vol_summary_1 = self.striplb(self.chars(tree.xpath('//*[@class="volunteering experienced"]//*[@class="experience  first"]//*[@class="summary"]//*[@class=" description"]//text()')))
                    locality_plus = self.striplb(self.chars(tree.xpath('//*[@class="demographic-info adr"]/dd[1]//text()')))
                    company_profile_link = self.striplb(self.chars(tree.xpath('//*[@class="position  first experience vevent vcard summary-current"]//*[@class="postitle"]//a[@class="company-profile-public"]/@href')))
                    skills = self.striplb(self.chars(tree.xpath('//*[@id="skills-list"]//text()')))
                    groups = self.striplb(self.chars(tree.xpath('//*[@class="groups"]//*[@class="group-data"]//*[@class="fn org"]/text()')))
                    cmain.writerow(id + title_current_1 + company_current_1 + org_stats_current_1 + date_start_current_1 + date_end_current_1 + duration_current_1 + location_current_1 + desc_current_1 + connections + first_name + last_name + title_comp_top + locality + industry + \
                                    summary + specialties + add_info_assoc + interests + honors + edu_school_1 + edu_degree_1 + edu_major_1 + edu_start_1 + edu_end_1 + edu_desc_1 + vol_title_1 + vol_org_1 + vol_specifics_1 + vol_start_1 + vol_stop_1 + vol_duration_1 + \
                                    vol_summary_1 + locality_plus + company_profile_link + skills + groups)
                except Exception:
                    print "error on: " + universal_path
            self.queue.task_done()
            

#define folder path
path_to_dir = raw_input(r'Location: ')
print path_to_dir
#define CSV files
csvdate = datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '.csv' #time_variable
f = open('liparse_THREADED_' + csvdate, "wb")
cmain = csv.writer(f, quoting=csv.QUOTE_MINIMAL) #actual csv file
#make CSV headers
cmain.writerow(['id', 'title_current_1', 'company_current_1', 'org_stats_current_1', 'date_start_current_1', 'date_end_current_1', 'duration_current_1', 'location_current_1', 'desc_current_1', 'connections', 'first_name', 'last_name', 'title_comp_top', 'locality', 'industry', \
                'summary', 'specialties', 'add_info_assoc', 'interests', 'honors', 'edu_school_1', 'edu_degree_1', 'edu_major_1', 'edu_start_1', 'edu_end_1', 'edu_desc_1', 'vol_title_1', 'vol_org_1', 'vol_specifics_1', 'vol_start_1', 'vol_stop_1', 'vol_duration_1', \
                'vol_summary_1', 'locality_plus', 'company_profile_link', 'skills', 'groups'])
#start timer
start = time.time()
print 'starting at ' + datetime.datetime.now().strftime('%H:%M:%S')


def main(): 
    
    #spawn a pool of threads, and pass them queue instance    
    for i in range(7):
        t = ThreadUrl(queue, out_queue)
        t.setDaemon(True)
        t.start()
        
    #populate queue with data
    for path, subdirs, files in os.walk(path_to_dir):
        #for file_in_dir in files_in_dir:
        for file in files:
            if file.endswith('.html'):
                universal_path = os.path.join(path, file)
                queue.put([universal_path,file])
            
    #wait on the queue until everything has been processed
    queue.join()
    out_queue.join()
main()

f.close()
print 'finished in ' + str((time.time() - start) / 60) + 'mins at ' + datetime.datetime.now().strftime('%H:%M:%S')
raw_input()
main()


