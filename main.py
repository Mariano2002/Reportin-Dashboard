# -*- coding: utf-8 -*-
import csv
import time
from datetime import date
import datetime
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tkinter import *
from threading import Thread
from tkinter import filedialog
import tkinter.messagebox as tkMessageBox
import shutil
from PIL import Image, ImageTk
import tkinter.ttk as ttk

def switch12():

    mainframe1.pack_forget()

    mainframe2.pack(anchor=N, fill=BOTH, expand=True, side=TOP,  padx=(40,10), pady=(0,0))


def switch21():
    mainframe2.pack_forget()

    mainframe1.pack(anchor=N, fill=BOTH, expand=True, side=TOP,  padx=(40,10), pady=(0,0))



def switch23():

    mainframe2.pack_forget()

    mainframe3.pack(anchor=N, fill=BOTH, expand=True, side=TOP,  padx=(40,10), pady=(0,0))



def switch32():

    mainframe3.pack_forget()

    mainframe2.pack(anchor=N, fill=BOTH, expand=True, side=TOP,  padx=(40,10), pady=(0,0))



def switch34():

    mainframe3.pack_forget()

    mainframe4.pack(anchor=N, fill=BOTH, expand=True, side=TOP,  padx=(10,10), pady=(0,0))



def switch43():

    mainframe4.pack_forget()

    mainframe3.pack(anchor=N, fill=BOTH, expand=True, side=TOP,  padx=(40,10), pady=(0,0))




def get_data():
    filename = filename1
    last_date = DATE1.get()
    with open(filename, encoding='utf-8-sig', newline='') as f:
        reader = csv.reader((line.replace('\0','') for line in f), delimiter=",")
        next(reader)
        for nr,row in enumerate(reader):
            try:
                if datetime.strptime('/'.join(x.zfill(2) for x in row[66].split('/')), '%m/%d/%Y') >= datetime.strptime('/'.join(x.zfill(2) for x in last_date.split('/')), '%m/%d/%Y'):
                    if row[6] == "ISIN":
                        input_data.append({'provider':row[0], "code":row[8], 'name':row[3], 'type':'ISIN'})
                    elif row[6] == "CUSIP":
                        input_data.append({'provider':row[0], "code":row[7], 'name':row[3], 'type':'Cusip'})
                    else:
                        if row[7] != "":
                            input_data.append({'provider':row[0], "code":row[7], 'name':row[3], 'type':'ISIN'})
                        elif row[8] != "":
                            input_data.append({'provider':row[0], "code":row[8], 'name':row[3], 'type':'Cusip'})
            except:
                pass



def get_data_comp():
    filename = filename5
    last_date = DATE2.get()
    print(scraped_data)
    with open(filename, encoding='utf-8-sig', newline='') as f:
        reader = csv.reader((line.replace('\0','') for line in f), delimiter=",")
        next(reader)
        for nr,row in enumerate(reader):
            try:
                if datetime.strptime('/'.join(x.zfill(2) for x in row[7].split('/')), '%m/%d/%Y') >= datetime.strptime('/'.join(x.zfill(2) for x in last_date.split('/')), '%m/%d/%Y'):
                    if row[3] != "":
                        for u in scraped_data:
                            if row[3].lower() == u['code'].lower():
                                u['old_link'] = row[5]
                    if row[2] != "":
                        for u in scraped_data:
                            if row[2].lower() == u['code'].lower():
                                u['old_link'] = row[5]
            except:
                pass


def add():
    code_v = CODE.get()
    provider = NAME.get()
    fund_name = FNAME.get()
    type = TYPEC.get()
    CODE.set("")
    NAME.set("")
    FNAME.set("")
    TYPEC.set(code_types[0])
    if code_v != "" and fund_name != "" and provider != "":
        input_data.append({'provider':provider, 'name':fund_name, "code":code_v, 'type':type})
    else:
        tkMessageBox.showerror("Invalid data", "Please complete at least one pair of data!")
        return



def scraper():
    global input_data, scraped_data

    csvf1.config(state='disable')
    date1.config(state='disable')
    pdf.config(state='disable')
    name.config(state='disable')
    fname.config(state='disable')
    typec.config(state='disable')
    add_btn1.config(state='disable')
    code1.config(state="disable")
    upload_btn.config(state="disable")
    download_btn.config(state="disable")
    download_btn2.config(state="disable")
    csvf2.config(state="disable")
    run.config(state="disable")
    csvf3.config(state="disable")
    date2.config(state="disable")
    run.config(bg="orange")
    run.config(text='Scraping')

    try:
        get_data()
    except:
        if input_data == []:
            csvf1.config(state='normal')
            date1.config(state='normal')
            pdf.config(state='normal')
            name.config(state='normal')
            fname.config(state='normal')
            typec.config(state='normal')
            add_btn1.config(state='normal')
            code1.config(state="normal")
            upload_btn.config(state="normal")
            download_btn.config(state="normal")
            download_btn2.config(state="normal")
            csvf2.config(state="normal")
            compare.config(state="normal")
            run.config(state="normal")
            csvf3.config(state="normal")
            date2.config(state="normal")
            run.config(bg="LightBlue1")
            run.config(text='Run Scraper')
            tkMessageBox.showerror("Invalid data", "Please complete at least one pair of data!")
            return







    whitelist_l = []
    with open(whitelist, encoding='utf-8-sig', newline='') as f:
        reader = csv.reader((line.replace('\0', '') for line in f), delimiter=",")
        for nr, row in enumerate(reader):
            if row[2] == '1':
                whitelist_l.append(row[1])
    # f = open('Output.csv', 'w', newline='', encoding='UTF-8')
    # writer = csv.writer(f)
    # writer.writerow(['Fund Provider', 'Fund Name', 'ISIN', 'Cusip', 'Document Type', 'Link', 'Working', 'Document kind'])
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(options=options)
    scraped_data = []

    for i in input_data:
        driver.get("https://www.google.com/")
        driver.get("https://www.google.com/search?q="+(i['provider']+" "+i['code']).replace(" ","+")+"&oq=Aberdeen+Standard+Investments+LU0779217297&sourceid=chrome&ie=UTF-8")
        c_link = driver.current_url
        link_m = ""
        working = "0"
        document = "link"
        try:
            nr_res = len(WebDriverWait(driver, 10).until(EC.visibility_of_all_elements_located((By.CLASS_NAME, 'g'))))
        except:
            nr_res = 0
        for list_nr in range(0,nr_res):
            document = "link"
            try:
                try:
                    if i['code'] in WebDriverWait(driver, 10).until(EC.visibility_of_all_elements_located((By.CLASS_NAME, 'g')))[list_nr].find_element_by_class_name("TXwUJf").text:
                       continue
                except:
                    pass
                link = WebDriverWait(driver, 10).until(EC.visibility_of_all_elements_located((By.CLASS_NAME, 'g')))[list_nr].find_element_by_tag_name("a").get_attribute("href")
                conti = 0
                for linki in whitelist_l:
                    if linki in link:
                        conti = 1
                        continue
                if conti == 1:
                    continue
                driver.get(link)

                if driver.current_url == c_link or driver.current_url[-4:]==".pdf":
                    if PDF.get() == 1:
                        document = "pdf"
                    elif PDF.get() == 0:
                        continue
                working = "1"
                link_m = link
                break
            except Exception as e:
                driver.get("https://www.google.com/")
                driver.get("https://www.google.com/search?q="+(i['provider']+" "+i['code']).replace(" ","+")+"&oq=Aberdeen+Standard+Investments+LU0779217297&sourceid=chrome&ie=UTF-8")
                link = ""
                working = "0"
                document = "link"
        if "This site can’t be reached" in driver.page_source:
            working = "0"
        if i['type'] == 'ISIN':
            scraped_data.append({"provider":i['provider'], "name":i['name'], "code":i['code'], "type":"isin", "document":document, "link":link_m, "working":working, "old_link":"", 'checked':"1"})
        elif i['type'] == 'Cusip':
            scraped_data.append({"provider":i['provider'], "name":i['name'], "code":i['code'], "type":"cusip", "document":document, "link":link_m, "working":working, "old_link":"", 'checked':"1"})
        time.sleep(2)

    # code.config(state='normal')
    # name.config(state='normal')
    # csvf.config(state='normal')
    # typec.config(state='normal')
    # date.config(state='normal')
    # justone_btn.config(state='normal')
    # add_btn.config(state='normal')
    # download.config(state='normal')
    # start_btn.config(bg='LightBlue1')
    # start_btn.config(text='Start')
    # start_btn.config(state='normal')
    input_data = []
    csvf1.config(state='normal')
    date1.config(state='normal')
    pdf.config(state='normal')
    name.config(state='normal')
    fname.config(state='normal')
    typec.config(state='normal')
    add_btn1.config(state='normal')
    code1.config(state="normal")
    upload_btn.config(state="normal")
    download_btn.config(state="normal")
    download_btn2.config(state="normal")
    csvf2.config(state="normal")
    compare.config(state="normal")
    run.config(state="normal")
    csvf3.config(state="normal")
    date2.config(state="normal")
    run.config(bg="LightBlue1")
    run.config(text='Run Scraper')



def start():
    thread = Thread(target=scraper)
    thread.start()

if __name__ == '__main__':

    input_data = []
    scraped_data = []
    whitelist = "whitelist.txt"
    types = [
        "1. Fact Sheet",
        "2. Holdings Report",
        "3. Daily Holdings Report",
        "4. Prospectus",
        "5. Annual Report",
        "6. Semi-Annual Report",
        "7. Statement of Addition Information"
    ]


    root = Tk()
    root.title("Fund Document Scraper \\ Menu")
    root.geometry("%dx%d+0+0" % (1400, 900))
    root.resizable(1, 1)
    root.config(bg="#3399ff")


    title = Frame(root, bd=1, relief=SOLID)
    title.pack(side=TOP, pady=10)
    lbl_display = Label(title, text="Fund Document Verification Tool", font=('arial', 20))
    lbl_display.pack(padx=240, pady=(5,35))
    mainframe1 = Frame(root, bg='#3399ff')
    mainframe1.pack(anchor=N, fill=BOTH, expand=True, side=TOP,  padx=(40,10), pady=(0,0))


###############################################################################################################################################################################################################################

    ###############################  VARIABLES  ##############################
    filename1 = None
    DATE1 = StringVar()
    PDF = IntVar()
    NAME = StringVar()
    FNAME = StringVar()
    TYPEC = StringVar()
    CODE = StringVar()


    def UploadAction1(event=None):
        global filename1
        filename1 = filedialog.askopenfilename()
        if filename1 != "" and filename1[-4:] == ".csv":
            csvf1.config(text=filename1.split("/")[-1])
        else:
            csvf1.config(text="Select file")
            filename1 = None


    lbl_csvf1 = Label(mainframe1, bg='#3399ff', text="Input file", font=('arial', 12), bd=10)
    lbl_csvf1.grid(row=0, pady=(75,0), sticky=W)
    csvf1 = Button(mainframe1, text='Select file', width=38, command=UploadAction1)
    csvf1.grid(row=0, column=1, pady=(75,0), sticky=W)



    def handle_focus_in1(_):
        if DATE1.get() == "MM/DD/YYYY" or DATE1.get() == "Example: 12/31/2021":
            date1.delete(0, END)
            date1.config(fg='black')

    def handle_focus_out1(_):
        if DATE1.get() == "":
            date1.delete(0, END)
            date1.config(fg='grey')
            date1.insert(0, "Example: 12/31/2021")


    lbl_date1 = Label(mainframe1, bg='#3399ff', text="Effective Date:", font=('arial', 12), bd=10)
    lbl_date1.grid(row=1, sticky=W)
    date1 = Entry(mainframe1, bg='white', textvariable=DATE1, font=('arial', 12), width=30, fg='grey')
    date1.grid(row=1, column=1, sticky=W)
    date1.insert(0, "MM/DD/YYYY")
    date1.bind("<FocusIn>", handle_focus_in1)
    date1.bind("<FocusOut>", handle_focus_out1)


    lbl_pdf = Label(mainframe1, bg='#3399ff', text="Scrape PDF's", font=('arial', 12), bd=10)
    lbl_pdf.grid(row=2, sticky=W)
    pdf = Checkbutton(mainframe1, variable=PDF, onvalue=1, offvalue=0)
    pdf.grid(row=2, column=1, sticky=W)
    pdf.config(bg="#3399ff")


    lbl = Label(mainframe1, bg='#3399ff', text="___________________________________________________________________________________________________________________________________", font=('arial', 12), bd=10)
    lbl.grid(row=3,columnspan=50, pady=(60,0), padx=(0,0))
    lbl = Label(mainframe1, bg='#3399ff', text="Manually add funds to Scraper", font=('arial', 12), bd=10)
    lbl.grid(row=4,columnspan=50, sticky=W)


    lbl_name = Label(mainframe1, bg='#3399ff', text="Provider Name:", font=('arial', 12), bd=10)
    lbl_name.grid(row=5, sticky=W)
    name = Entry(mainframe1, bg='white', textvariable=NAME, font=('arial', 12), width=30)
    name.grid(row=5, column=1, sticky=W)


    lbl_fname = Label(mainframe1, bg='#3399ff', text="Fund Name:", font=('arial', 12), bd=10)
    lbl_fname.grid(row=6, sticky=W)
    fname = Entry(mainframe1, bg='white', textvariable=FNAME, font=('arial', 12), width=30)
    fname.grid(row=6, column=1, sticky=W)

    lbl_typec = Label(mainframe1, bg='#3399ff', text="Fund Code Type:", font=('arial', 12), bd=10)
    lbl_typec.grid(row=7, sticky=W)
    code_types = ['ISIN', 'Cusip']
    typec = OptionMenu(mainframe1, TYPEC, *code_types)
    typec.config(width=38)
    typec.grid(row=7, column=1, sticky=W)
    TYPEC.set(code_types[0])

    lbl_code1 = Label(mainframe1, bg='#3399ff', text="Cusip/ISIN:", font=('arial', 12), bd=10)
    lbl_code1.grid(row=8, sticky=W)
    code1 = Entry(mainframe1, bg='white', textvariable=CODE, font=('arial', 12), width=30)
    code1.grid(row=8, column=1, sticky=W)

    def add1():
        code_v = CODE.get()
        provider = NAME.get()
        fund_name = FNAME.get()
        type = TYPEC.get()
        CODE.set("")
        NAME.set("")
        FNAME.set("")
        TYPEC.set(code_types[0])
        if code_v != "" and fund_name != "" and provider != "":
            input_data.append({'provider':provider, "code":code_v, 'name':fund_name, 'type':type})
        else:
            tkMessageBox.showerror("Invalid data", "Please complete at least one pair of data!")
            return

    add_btn1 = Button(mainframe1, text='Add Fund', width=10, command=add1)
    add_btn1.grid(row=9, column=1, sticky=W)
    add_btn1.config(bg="LightBlue1")

    # justone_btn = Button(mainframe1, text='Just this', width=20)
    # justone_btn.config(bg="LightBlue1")
    #
    # start_btn = Button(mainframe1, text='Start', width=20)
    # start_btn.config(bg="LightBlue1")
    #
    # download = Button(mainframe1, text="Download", width=20, bg="#009ACD")
    # download.config(bg="LightBlue1")
    # download.config(state='disable')



    image11 = Image.open("right.png")
    image11 = image11.resize((25, 25), Image.ANTIALIAS)
    photo11 = ImageTk.PhotoImage(image11)
    right_button1 = Button(mainframe1, image=photo11, border=0, command=switch12)
    right_button1.config(bg="#3399ff")
    right_button1.place(x=670, y=725)

    image21 = Image.open("left.png")
    image21 = image21.resize((25, 25), Image.ANTIALIAS)
    photo21 = ImageTk.PhotoImage(image21)
    left_button1 = Button(mainframe1, image=photo21, border=0)
    left_button1.config(bg="#3399ff")
    left_button1.place(x=630, y=725)



###############################################################################################################################################################################################################################

    ################### VARIABLE #####################
    filename2 = StringVar()

    mainframe2 = Frame(root, bg='#3399ff')
    # mainframe2.pack(anchor=N, fill=BOTH, expand=True, side=TOP,  padx=(40,10), pady=(0,0))

    lbl = Label(mainframe2, bg='#3399ff', text="Check Fund Provider Website Whitelist", font=('arial', 10), bd=11)
    lbl.place(x=205, y=0)


    def DisplayData1():
        global color_number, checked_or_not
        color_number = True
        checked_or_not = True
        fetch = []
        with open(whitelist, encoding='utf-8-sig', newline='') as f:
            reader = csv.reader((line.replace('\0','') for line in f), delimiter=",")
            for nr,row in enumerate(reader):
                fetch.append(row)
        for data in fetch:
            if data[-1] == "0":
                checked_or_not = False
            elif data[-1] == "1":
                checked_or_not = True
            elif data[-1] == "":
                checked_or_not = False

            if color_number == True:
                tree1.insert('', 'end', values=(data), tags=('gr',),)
                color_number = False
            else:
                tree1.insert('', 'end', values=(data),)
                color_number = True


    def live_edit(event):
        global entryedit
        try:
            entryedit.destroy()
        except:
            pass
        column = tree1.identify_column(event.x)
        if column == "#4":
            return
        row = tree1.identify_row(event.y)
        cols = ('#0', ) + tree1.cget('columns')
        n_width = tree1.column(cols[int(column.replace("#",""))], 'width')
        x_long = 0
        for i in range(0,int(column.replace("#",""))):
            x_long = x_long + tree1.column(cols[i], 'width')
        if(row == ""):
            return
        item = tree1.selection()[0]
        item_text = tree1.item(item, "values")[int(column.replace("#",""))-1]
        entryedit = Text(mainframe2)
        entryedit.insert("1.0", item_text)
        entryedit.place(x=x_long, y=75+tree1.index(item)*30, width=n_width, height=30)
        entryedit.focus()

        def saveedit(event):
            value = entryedit.get("1.0",END).replace("\n","")
            entryedit.destroy()
            list_check = []
            if (column == "#2"):
                for child in tree1.get_children():
                    list_check.append(tree1.item(child)["values"][1])
                if list_check.count(value) > 0:
                    pass
                else:
                    tree1.set(item, column=column, value=value)
            else:
                tree1.set(item, column=column, value=value)


        entryedit.bind("<FocusOut>", saveedit)
        entryedit.bind("<Return>", saveedit)



    def fixed_map(option):
        return [elm for elm in style.map("Treeview", query_opt=option)
                if elm[:2] != ("!disabled", "!selected")]


    class CbTreeview(ttk.Treeview):
        def __init__(self, master=None, **kw):
            kw.setdefault('style', 'cb.Treeview')
            kw.setdefault('show', 'headings')  # hide column #0
            ttk.Treeview.__init__(self, master, **kw)
            # create checheckbox images
            self._im_checked = PhotoImage('checked',
                                             data=b'GIF89a\x0e\x00\x0e\x00\xf0\x00\x00\x00\x00\x00\x00\x00\x00!\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x0e\x00\x0e\x00\x00\x02#\x04\x82\xa9v\xc8\xef\xdc\x83k\x9ap\xe5\xc4\x99S\x96l^\x83qZ\xd7\x8d$\xa8\xae\x99\x15Zl#\xd3\xa9"\x15\x00;',
                                             master=self)
            self._im_unchecked = PhotoImage('unchecked',
                                               data=b'GIF89a\x0e\x00\x0e\x00\xf0\x00\x00\x00\x00\x00\x00\x00\x00!\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x0e\x00\x0e\x00\x00\x02\x1e\x04\x82\xa9v\xc1\xdf"|i\xc2j\x19\xce\x06q\xed|\xd2\xe7\x89%yZ^J\x85\x8d\xb2\x00\x05\x00;',
                                               master=self)
            style = ttk.Style(self)
            style.configure("cb.Treeview.Heading", font=(None, 13))
            # put image on the right
            style.layout('cb.Treeview.Row',
                         [('Treeitem.row', {'sticky': 'nswe'}),
                          ('Treeitem.image', {'side': 'right', 'sticky': 'e'})])

            # use tags to set the checkbox state
            self.tag_configure('checked', image='checked')
            self.tag_configure('unchecked', image='unchecked')

        def tag_add(self, item, tags):
            new_tags = tuple(self.item(item, 'tags')) + tuple(tags)
            self.item(item, tags=new_tags)

        def tag_remove(self, item, tag):
            tags = list(self.item(item, 'tags'))
            tags.remove(tag)
            self.item(item, tags=tags)

        def insert(self, parent, index, iid=None, **kw):
            item = ttk.Treeview.insert(self, parent, index, iid, **kw)
            if checked_or_not == False:
                self.tag_add(item, (item, 'unchecked'))
                self.tag_bind(item, '<ButtonRelease-1>',
                              lambda event: self._on_click(event, item))
            elif checked_or_not == True:
                self.tag_add(item, (item, 'checked'))
                self.tag_bind(item, '<ButtonRelease-1>',
                              lambda event: self._on_click(event, item))

        def _on_click(self, event, item):
            """Handle click on items."""
            if self.identify_row(event.y) == item:
                if self.identify_column(event.x) == '#4':  # click in 'Served' column
                    # toggle checkbox image
                    if self.tag_has('checked', item):
                        self.tag_remove(item, 'checked')
                        self.tag_add(item, ('unchecked',))
                        self.set(item, column='#3', value=0)
                    else:
                        self.tag_remove(item, 'unchecked')
                        self.tag_add(item, ('checked',))
                        self.set(item, column='#3', value=1)


    style = ttk.Style()
    style.configure('Treeview', rowheight=30)
    style.map("Treeview",
              foreground=fixed_map("foreground"),
              background=fixed_map("background"))



    scrollbary = Scrollbar(mainframe2, orient=VERTICAL)
    tree1 = CbTreeview(mainframe2, columns=(
    "Fund Provider", "Website Link", "", "Checkmark"), selectmode="extended",
                        height=100, yscrollcommand=scrollbary.set)

    tree1.tag_configure('gr', background='#F9F9F9')
    scrollbary.config(command=tree1.yview)
    scrollbary.pack(side=RIGHT, pady=(50,130), fill=Y)

    tree1.heading('Fund Provider', text="Fund Provider", anchor=W)
    tree1.heading('Website Link', text="Website Link", anchor=W)
    tree1.heading('', text="", anchor=W)
    tree1.heading('Checkmark', text="Checkmark", anchor=W)
    tree1.column('#0', stretch=NO, minwidth=0, width=0)
    tree1.column('#1', stretch=NO, minwidth=0, width=340)
    tree1.column('#2', stretch=NO, minwidth=0, width=845)
    tree1.column('#3', stretch=NO, minwidth=0, width=0)
    tree1.column('#4', stretch=NO, minwidth=0, width=145)
    tree1.pack(side=BOTTOM, fill=X, pady=(50,130))
    tree1.bind('<Double-1>', live_edit)


    DisplayData1()

    def UploadAction2(event=None):
        global whitelist
        whitelist = filedialog.askopenfilename()
        if whitelist != "" and whitelist[-4:]==".txt":
            tree1.delete(*tree1.get_children())
            DisplayData1()
        else:
            whitelist = "whitelist.txt"
        save1()

    upload_btn2 = Button(mainframe2, text='Upload Whitelist', width=20, command=UploadAction2)
    upload_btn2.place(x=1180, y=10)
    upload_btn2.config(bg="LightBlue1")



    def new_row():
        global color_number
        do = 1
        for child in tree1.get_children():
            valuesPE = tree1.item(child)["values"]
            if(valuesPE[1]==""):
                do = 0
                tkMessageBox.showerror("Error", "Please enter Full Name!")
                break
        if(do == 1):
            if color_number == True:
                tree1.insert('', len(tree1.get_children()), values=("", "", "", ""), tags=('gr',))
                color_number = False
            else:
                tree1.insert('', len(tree1.get_children()), values=("", "", "", ""))
                color_number = True
            tree1.update()
            child_id = tree1.get_children()[-1]
            tree1.selection_set(child_id)

    add_btn2 = Button(mainframe2, text='Add Website', width=20, command=new_row)
    add_btn2.place(x=1180, y=685)
    add_btn2.config(bg="LightBlue1")


    def save1():
        file = open("whitelist.txt", 'w')
        for child in tree1.get_children():
            lista = tree1.item(child)["values"][:3]
            for nr,i in enumerate(lista):
                lista[nr] = str(i)
            file.write(",".join(lista)+"\n")
        file.close()

    save_btn = Button(mainframe2, text='Save Change', width=20, command=save1)
    save_btn.place(x=1180, y=725)
    save_btn.config(bg="LightBlue1")




    image12 = Image.open("right.png")
    image12 = image12.resize((25, 25), Image.ANTIALIAS)
    photo12 = ImageTk.PhotoImage(image12)
    right_button2 = Button(mainframe2, image=photo12, border=0, command=switch23)
    right_button2.config(bg="#3399ff")
    right_button2.place(x=670, y=725)

    image22 = Image.open("left.png")
    image22 = image22.resize((25, 25), Image.ANTIALIAS)
    photo22 = ImageTk.PhotoImage(image22)
    left_button2 = Button(mainframe2, image=photo22, border=0, command=switch21)
    left_button2.config(bg="#3399ff")
    left_button2.place(x=630, y=725)


    mainframe2.pack_forget()

###############################################################################################################################################################################################################################

    mainframe3 = Frame(root, bg='#3399ff')
    # mainframe3.pack(anchor=N, fill=BOTH, expand=True, side=TOP,  padx=(40,10), pady=(0,0))

    ##################### VARIABLE #####################
    DATE2 = StringVar()
    filename3 = None
    filename4 = None
    filename5 = None



    lbl = Label(mainframe3, bg='#3399ff', text="Additional Functions", font=('arial', 11), bd=10)
    lbl.place(x=570, y=0)


    def UploadAction3(event=None):
        global whitelist
        whitelist = filedialog.askopenfilename()
        if whitelist != "" and whitelist[-4:]==".txt":
            tree1.delete(*tree1.get_children())
            DisplayData1()
        else:
            whitelist = "whitelist.txt"
        save1()

    upload_btn = Button(mainframe3, text='Upload Whitelist', width=20, command=UploadAction3)
    upload_btn.place(x=480, y=50)
    upload_btn.config(bg="LightBlue1")


    def download1():

        f = filedialog.asksaveasfile(title='Name a file', initialdir='C:\\', filetypes=(("Text File", "*.txt*"),),
                                     defaultextension='.txt')
        if f is None:
            return

        if f.name[-4:] == ".txt":
            original = './whitelist.txt'
            target = f.name

            shutil.copyfile(original, target)
            tkMessageBox.showinfo(title="Downloaded", message="The Whitelist file is downloaded.", )


    download_btn = Button(mainframe3, text='Download Whitelist', width=20, command=download1)
    download_btn.place(x=480, y=90)
    download_btn.config(bg="LightBlue1")



    def download2():
        f = open('scraper_file.csv', 'w', newline='', encoding='UTF-8')
        writer = csv.writer(f)
        writer.writerow(['Fund Provider', 'Fund Name', 'ISIN', 'Cusip', 'Document Type', 'Link', 'Working', 'Document kind'])

        for i in scraped_data:
            for typ in types:
                if i['type'] == 'isin':
                    writer.writerow([i['provider'], i['name'], i['code'], '', i['document'], i['link'], i['working'], typ])
                elif i['type'] == 'cusip':
                    writer.writerow([i['provider'], i['name'], '', i['code'], i['document'], i['link'], i['working'], typ])
        f.close()

        f = filedialog.asksaveasfile(title='Name a file', initialdir='C:\\', filetypes=(("Comma Delimited", "*.csv*"),),
                                     defaultextension='.csv')
        if f is None:
            return

        if f.name[-4:] == ".csv":
            original = r'./scraper_file.csv'
            target = f.name

            shutil.copyfile(original, target)
            tkMessageBox.showinfo(title="Downloaded", message="The Excel file is downloaded.", )


    download_btn2 = Button(mainframe3, text='Download Scraper File', width=20, command=download2)
    download_btn2.place(x=480, y=130)
    download_btn2.config(bg="LightBlue1")

    def UploadAction4(event=None):
        global filename4
        filename4 = filedialog.askopenfilename()
        if filename4 != "":
            csvf1.config(text=filename4.split("/")[-1])
        else:
            csvf1.config(text="Select file")
            filename4 = None


    csvf2 = Button(mainframe3, text='Select file', width=38, command=UploadAction4)
    csvf2.place(x=665, y=50)




    run = Button(mainframe3, text="Run Scraper", width=20, height=2, bg="#009ACD", command=start)
    run.config(bg="LightBlue1")
    run.place(x=665, y=220)


    lbl = Label(mainframe3, bg='#3399ff', text="Compare to Fund Connect Fund Document Export", font=('arial', 11), bd=10)
    lbl.place(x=550, y=275)


    def UploadAction5(event=None):
        global filename5
        filename5 = filedialog.askopenfilename()
        if filename5 != "":
            csvf3.config(text=filename5.split("/")[-1])
        else:
            csvf3.config(text="Select file")
            filename5 = None

    lbl_csvf3 = Label(mainframe3, bg='#3399ff', text="Input file", font=('arial', 12), bd=10)
    lbl_csvf3.place(x=500, y=320)
    csvf3 = Button(mainframe3, text='Select file', width=38, command=UploadAction5)
    csvf3.place(x=665, y=325)




    def handle_focus_in(_):
        if DATE2.get() == "MM/DD/YYYY" or DATE2.get() == "Example: 12/31/2021":
            date2.delete(0, END)
            date2.config(fg='black')

    def handle_focus_out(_):
        if DATE2.get() == "":
            date2.delete(0, END)
            date2.config(fg='grey')
            date2.insert(0, "Example: 12/31/2021")



    lbl_date2 = Label(mainframe3, bg='#3399ff', text="Effective Date:", font=('arial', 12), bd=10)
    lbl_date2.place(x=500, y=360)
    date2 = Entry(mainframe3, bg='white', textvariable=DATE2, font=('arial', 12), width=30, fg='grey')
    date2.place(x=665, y=365)
    date2.insert(0, "MM/DD/YYYY")
    date2.bind("<FocusIn>", handle_focus_in)
    date2.bind("<FocusOut>", handle_focus_out)


    def compare():
        get_data_comp()
        DisplayData2()

    compare = Button(mainframe3, text="Compare", width=20, height=2, bg="#009ACD", command=compare)
    compare.config(bg="LightBlue1")
    compare.place(x=665, y=430)
    compare.config(state="disabled")

    lbl = Label(mainframe3, bg='#3399ff', text="Click to next view after compare ran", font=('arial', 10), bd=10)
    lbl.place(x=550, y=660)


    image13 = Image.open("right.png")
    image13 = image13.resize((25, 25), Image.ANTIALIAS)
    photo13 = ImageTk.PhotoImage(image13)
    right_button3 = Button(mainframe3, image=photo13, border=0, command=switch34)
    right_button3.config(bg="#3399ff")
    right_button3.place(x=670, y=725)

    image23 = Image.open("left.png")
    image23 = image23.resize((25, 25), Image.ANTIALIAS)
    photo23 = ImageTk.PhotoImage(image23)
    left_button3 = Button(mainframe3, image=photo23, border=0, command=switch32)
    left_button3.config(bg="#3399ff")
    left_button3.place(x=630, y=725)

###############################################################################################################################################################################################################################


    mainframe4 = Frame(root, bg='#3399ff')
    # mainframe4.pack(anchor=N, fill=BOTH, expand=True, side=LEFT,  padx=(40,10), pady=(0,0))



    def DisplayData2():
        global color_number, checked_or_not2


        color_number = True
        checked_or_not2 = True
        fetch = []

        for i in scraped_data:
            checked_or_not2 = i['checked']
            if i['checked'] == "0":
                checked_or_not2 = False
            elif i['checked'] == "1":
                checked_or_not2 = True
            elif i['checked'] == "":
                checked_or_not2 = False

            data = []
            if i['type'] == 'cusip':
                data = [i['provider'], i['name'], '', i['code'], i['document'], i['link'], i['working'], '', i['old_link'], i['checked']]
            elif i['type'] == 'isin':
                data = [i['provider'], i['name'], i['code'], '', i['document'], i['link'], i['working'], '', i['old_link'], i['checked']]

            if color_number == True:
                tree2.insert('', 'end', values=(data), tags=('gr',),)
                color_number = False
            else:
                tree2.insert('', 'end', values=(data),)
                color_number = True


    def fixed_map(option):
        return [elm for elm in style.map("Treeview", query_opt=option)
                if elm[:2] != ("!disabled", "!selected")]


    class CbTreeview2(ttk.Treeview):
        def __init__(self, master=None, **kw):
            kw.setdefault('style', 'cb.Treeview')
            kw.setdefault('show', 'headings')  # hide column #0
            ttk.Treeview.__init__(self, master, **kw)
            # create checheckbox images
            self._im_checked = PhotoImage('checked',
                                             data=b'GIF89a\x0e\x00\x0e\x00\xf0\x00\x00\x00\x00\x00\x00\x00\x00!\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x0e\x00\x0e\x00\x00\x02#\x04\x82\xa9v\xc8\xef\xdc\x83k\x9ap\xe5\xc4\x99S\x96l^\x83qZ\xd7\x8d$\xa8\xae\x99\x15Zl#\xd3\xa9"\x15\x00;',
                                             master=self)
            self._im_unchecked = PhotoImage('unchecked',
                                               data=b'GIF89a\x0e\x00\x0e\x00\xf0\x00\x00\x00\x00\x00\x00\x00\x00!\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x0e\x00\x0e\x00\x00\x02\x1e\x04\x82\xa9v\xc1\xdf"|i\xc2j\x19\xce\x06q\xed|\xd2\xe7\x89%yZ^J\x85\x8d\xb2\x00\x05\x00;',
                                               master=self)
            style = ttk.Style(self)
            style.configure("cb.Treeview.Heading", font=(None, 13))
            # put image on the right
            style.layout('cb.Treeview.Row',
                         [('Treeitem.row', {'sticky': 'nswe'}),
                          ('Treeitem.image', {'side': 'right', 'sticky': 'e'})])

            # use tags to set the checkbox state
            self.tag_configure('checked', image='checked')
            self.tag_configure('unchecked', image='unchecked')

        def tag_add(self, item, tags):
            new_tags = tuple(self.item(item, 'tags')) + tuple(tags)
            self.item(item, tags=new_tags)

        def tag_remove(self, item, tag):
            tags = list(self.item(item, 'tags'))
            tags.remove(tag)
            self.item(item, tags=tags)

        def insert(self, parent, index, iid=None, **kw):
            item = ttk.Treeview.insert(self, parent, index, iid, **kw)
            if checked_or_not2 == False:
                self.tag_add(item, (item, 'unchecked'))
                self.tag_bind(item, '<ButtonRelease-1>',
                              lambda event: self._on_click(event, item))
            elif checked_or_not2 == True:
                self.tag_add(item, (item, 'checked'))
                self.tag_bind(item, '<ButtonRelease-1>',
                              lambda event: self._on_click(event, item))

        def _on_click(self, event, item):
            """Handle click on items."""
            if self.identify_row(event.y) == item:
                if self.identify_column(event.x) == '#11':  # click in 'Served' column
                    # toggle checkbox image
                    if self.tag_has('checked', item):
                        self.tag_remove(item, 'checked')
                        self.tag_add(item, ('unchecked',))
                        self.set(item, column='#10', value=0)
                    else:
                        self.tag_remove(item, 'unchecked')
                        self.tag_add(item, ('checked',))
                        self.set(item, column='#10', value=1)


    def check_all():
        global color_number, checked_or_not2

        color_number = True
        checked_or_not2 = True
        childrens = []
        for child in tree2.get_children():
            childrens.append(tree2.item(child)["values"])
        tree2.delete(*tree2.get_children())
        for child in childrens:
            print(child)
            child[9] = 1
            if color_number == True:
                tree2.insert('', 'end', values=(child), tags=('gr',),)
                color_number = False
            else:
                tree2.insert('', 'end', values=(child),)
                color_number = True


    def uncheck_all():
        global color_number, checked_or_not2

        color_number = True
        checked_or_not2 = False
        childrens = []
        for child in tree2.get_children():
            childrens.append(tree2.item(child)["values"])
        tree2.delete(*tree2.get_children())
        for child in childrens:
            print(child)
            child[9] = 0

            if color_number == True:
                tree2.insert('', 'end', values=(child), tags=('gr',),)
                color_number = False
            else:
                tree2.insert('', 'end', values=(child),)
                color_number = True




    checkall = Button(mainframe4, text='Check All', width=20, command=check_all)
    checkall.place(x=1210, y=10)
    checkall.config(bg="LightBlue1")

    checkall = Button(mainframe4, text='Uncheck All', width=20, command=uncheck_all)
    checkall.place(x=1050, y=10)
    checkall.config(bg="LightBlue1")

    style = ttk.Style()
    style.configure('Treeview', rowheight=30)
    style.map("Treeview",
              foreground=fixed_map("foreground"),
              background=fixed_map("background"))



    scrollbary = Scrollbar(mainframe4, orient=VERTICAL)
    tree2 = CbTreeview2(mainframe4, columns=(
    "Fund Provider", "Fund Name", "ISIN", "Cusip", "Document", "Website Link", "Working", "Document Kind", "Link Currently in Fund", "", "Overwrite"), selectmode="extended",
                        height=100, yscrollcommand=scrollbary.set)

    tree2.tag_configure('gr', background='#F9F9F9')
    scrollbary.config(command=tree2.yview)
    scrollbary.pack(side=RIGHT, pady=(50,130), fill=Y)

    tree2.heading('Fund Provider', text="Fund Provider", anchor=W)
    tree2.heading('Fund Name', text="Fund Name", anchor=W)
    tree2.heading('ISIN', text="ISIN", anchor=W)
    tree2.heading('Cusip', text="Cusip", anchor=W)
    tree2.heading('Document', text="Document", anchor=W)
    tree2.heading('Website Link', text="Website Link", anchor=W)
    tree2.heading('Working', text="Working", anchor=W)
    tree2.heading('Document Kind', text="Document Kind", anchor=W)
    tree2.heading('Link Currently in Fund', text="Link Currently in Fund", anchor=W)
    tree2.heading('', text="", anchor=W)
    tree2.heading('Overwrite', text="Overwrite", anchor=W)
    tree2.column('#0', stretch=NO, minwidth=0, width=0)
    tree2.column('#1', stretch=NO, minwidth=0, width=180)
    tree2.column('#2', stretch=NO, minwidth=0, width=215)
    tree2.column('#3', stretch=NO, minwidth=0, width=90)
    tree2.column('#4', stretch=NO, minwidth=0, width=90)
    tree2.column('#5', stretch=NO, minwidth=0, width=85)
    tree2.column('#6', stretch=NO, minwidth=0, width=250)
    tree2.column('#7', stretch=NO, minwidth=0, width=70)
    tree2.column('#8', stretch=NO, minwidth=0, width=120)
    tree2.column('#9', stretch=NO, minwidth=0, width=180)
    tree2.column('#10', stretch=NO, minwidth=0, width=0)
    tree2.column('#11', stretch=NO, minwidth=0, width=80)
    tree2.pack(side=BOTTOM, fill=X, pady=(50,130))




    def save2():
        f = open('Output.csv', 'w', newline='', encoding='UTF-8')
        writer = csv.writer(f)
        writer.writerow(['Fund Provider', 'Fund Name', 'ISIN', 'Cusip', 'Document Type', 'Link', 'Working', 'Document kind'])

        for child in tree2.get_children():
            lista = tree2.item(child)["values"][:7]
            print(tree2.item(child)["values"])
            for nr,i in enumerate(lista):
                lista[nr] = str(i)
            if tree2.item(child)["values"][9] == 0 or lista[5] == "":
                lista[5] = tree2.item(child)["values"][8]
            for typ in types:
                writer.writerow(lista+[typ])
        f.close()


    save_btn2 = Button(mainframe4, text='Save', width=20, command=save2)
    save_btn2.place(x=1210, y=685)
    save_btn2.config(bg="LightBlue1")


    def export():

        f = filedialog.asksaveasfile(title='Name a file', initialdir='C:\\', filetypes=(("Comma Delimited", "*.csv*"),),
                                     defaultextension='.csv')
        if f is None:
            return

        if f.name[-4:] == ".csv":
            original = r'./Output.csv'
            target = f.name

            shutil.copyfile(original, target)
            tkMessageBox.showinfo(title="Downloaded", message="The Excel file is downloaded.", )


    export_btn = Button(mainframe4, text='Export', width=20, command=export)
    export_btn.place(x=1210, y=725)
    export_btn.config(bg="LightBlue1")




    image14 = Image.open("right.png")
    image14 = image14.resize((25, 25), Image.ANTIALIAS)
    photo14 = ImageTk.PhotoImage(image14)
    right_button4 = Button(mainframe4, image=photo14, border=0)
    right_button4.config(bg="#3399ff")
    right_button4.place(x=700, y=725)

    image24 = Image.open("left.png")
    image24 = image24.resize((25, 25), Image.ANTIALIAS)
    photo24 = ImageTk.PhotoImage(image24)
    left_button4 = Button(mainframe4, image=photo24, border=0, command=switch43)
    left_button4.config(bg="#3399ff")
    left_button4.place(x=660, y=725)









    root.mainloop()
