# This file contains styling for the reporting dashboard that is imported to the main code panel_process.py

# table_layout contains is a list that can be used to change the design of a pandas data frame
# this can be achieved by passing it as input to the function .set_table_styles
table_layout = [
    {'selector': '.bk-root.div.bk.div.bk.div.bk.div.bk.div.bk',
     'props': [('width', '1201px')]},
    {'selector': 'table',
     'props': [('margin-left', 'auto'),
               ('margin-right', 'auto'),
               ('border', 'none'),
               ('border-collapse', 'collapse'),
               ('border-spacing', '0'),
               ('font-size', '12px'),  # does not work
               ('table-layout', 'fixed'),
               ('width', '1000px')]},  # does not work
    {'selector': ['tr', 'th', 'td'],
     'props': [('text-align', 'right'),
               ('vertical-align', 'middle'),
               ('padding', '0.5em 0.5em !important'),
               ('line-height', 'normal'),
               ('white-space', 'normal'),
               ('width', '1100px'),  # does not work
               ('max-width', 'none'),  # does not work
               ('border', 'none')]},
    {'selector': 'tbody',
     'props': [('display', 'table-row-group'),
               ('vertical-align', 'middle'),
               ('border-color', 'inherit')]},
    {'selector': 'tbody tr:nth-child(odd)',
     'props': [('background', '#f5f5f5')]},
    {'selector': 'tr:last-child',
     'props': [('font-weight', 'bold'),
               ('border-bottom', '1px double black')]},
    {'selector': 'thead',
     'props': [('border-bottom', '1px solid black'),  # does not work
               ('vertical-align', 'bottom')]},  # does not work
    {'selector': 'tr:hover',
     'props': [('background', 'lightblue !important'),
               ('cursor', 'pointer')]}]

table_layout_no_total = [
    {'selector': '.bk-root.div.bk.div.bk.div.bk.div.bk.div.bk',
     'props': [('width', '1201px')]},
    {'selector': 'table',
     'props': [('margin-left', 'auto'),
               ('margin-right', 'auto'),
               ('border', 'none'),
               ('border-collapse', 'collapse'),
               ('border-spacing', '0'),
               ('font-size', '12px'),  # does not work
               ('table-layout', 'fixed'),
               ('width', '1000px')]},  # does not work
    {'selector': ['tr', 'th', 'td'],
     'props': [('text-align', 'right'),
               ('vertical-align', 'middle'),
               ('padding', '0.5em 0.5em !important'),
               ('line-height', 'normal'),
               ('white-space', 'normal'),
               ('width', '1100px'),  # does not work
               ('max-width', 'none'),  # does not work
               ('border', 'none')]},
    {'selector': 'tbody',
     'props': [('display', 'table-row-group'),
               ('vertical-align', 'middle'),
               ('border-color', 'inherit')]},
    {'selector': 'tbody tr:nth-child(odd)',
     'props': [('background', '#f5f5f5')]},
    {'selector': 'thead',
     'props': [('border-bottom', '1px solid black'),  # does not work
               ('vertical-align', 'bottom')]},  # does not work
    {'selector': 'tr:hover',
     'props': [('background', 'lightblue !important'),
               ('cursor', 'pointer')]}]

template1 = """
<div style="color:<%=
    (function colorfromint(){
        if(value > 0){
            return("green")}
        else if (value < 0 ){
            return("red")}
         else if (value == 0 ){
            return("black")}
        else{return("black")}
        }()) %>;
    ">
<%= parseFloat(value*100).toFixed(2) +" %" %></div>
"""

template2 = """
<div style="color:<%=
    (function colorfromint(){
    nfObject = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' })
        if(value > 0){
            return("green")}
        else if (value < 0 ){
            return("red")}
         else if (value == 0 ){
            return("black")}
        else{return("black")}
        }()) %>;
    ">
<%= nfObject.format(value/1000000) +" MM" %></div>
"""

template3 = """
<div style="color:<%=
    (function colorfromint(){
    nfObject = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' })
        if(value > 0){
            return("green")}
        else if (value < 0 ){
            return("red")}
         else if (value == 0 ){
            return("black")}
        else{return("black")}
        }()) %>;
    >
<%= nfObject.format(value)  %></div>
"""

template4 = """
<div style="color:<%=
    (function erf(){
    nfObject = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' })
        }()) %>;
    ">
<%= nfObject.format(value)  %></div>
"""

template5 = """
<div style="color:<%=
    (function erf(){
    nfObject = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' })
        }()) %>;
    ">
<%= nfObject.format(value/1000000) +" MM" %></div>
"""

template6 = """
<div style="color:<%=
    (function colorfromint(){
        if(value > 0){
            return("black")}
        else if (value < 0 ){
            return("black")}
         else if (value == 0 ){
            return("black")}
        else{return("black")}
        }()) %>;
    ">
<%= parseFloat(value*1).toFixed(6)%></div>
"""

template7 = """
<div style="color:<%=
    (function colorfromint(){
        if(value > 0){
            return("black")}
        else if (value < 0 ){
            return("black")}
         else if (value == 0 ){
            return("black")}
        else{return("black")}
        }()) %>;
    ">
<%= parseFloat(value*100).toFixed(2) +"%" %></div>
"""

template8 = """
<div style="color:<%=
    (function ifLessThan(){
        if(value < 0.01){
            return("red")}
        }()) %>;
    ">
<%= parseFloat(value*100).toFixed(4) +"%" %></div>
"""

bring_to_front = """
.content {
    position: absolute;
}
"""

mi_script = """
<script>
if (document.readyState === "complete") {
  $('.example').DataTable();
} else {
  $(document).ready(function () {
    $('.example').DataTable();
  })
}

</script>
"""

table_style = """


.table {
    margin-left: auto;
    margin-right: auto;
    border: none;
    border-collapse: collapse;
    border-spacing: 0;
    color: black;
    font-size: 12px;
    table-layout: fixed;
    width: 100%;
}

.tr, th, td {
    text-align: right;
    vertical-align: middle;
    padding: 0.5em 0.5em !important;
    line-height: normal;
    white-space: normal;
    max-width: none;
    border: none;
}

.tbody {
    display: table-row-group;
    vertical-align: middle;
    border-color: inherit;
}

.tbody tr:nth-child(odd) {
    background: #f5f5f5;
}

.thead {
    border-bottom: 1px solid black;
    vertical-align: bottom;
}

.tr:hover {
    background: lightblue !important;
    cursor: pointer;
}

"""

# CSS styling
css = '''
.panel-widget-box {
  background: #ffffff;
  border-radius: 5px;
  border: 1px black solid;
}
.panel-hover-tool {
  z-index: 10;
  padding-bottom: 15px;
}
.panel-hover-tool2 {
  z-index: 11;
}
.table-style {
width: 120%;
}
.table-style td {
width: 120%;
}

table.panel-df {
        margin-left: auto;
        margin-right: auto;
        color: black;
        border: none;
        border-collapse: collapse;
        border-spacing: 0;
        font-size: 12px;
        table-layout: auto;
        width: 120%;
    }

    .panel-df tr, .panel-df th, .panel-df td {
        text-align: right;
        vertical-align: middle;
        padding: 0.5em 0.5em !important;
        line-height: normal;
        white-space: normal;
        max-width: none;
        border: none;
        width: 15%;
    }

    .panel-df tbody {
        display: table-row-group;
        vertical-align: middle;
        border-color: inherit;
    }

    .panel-df tbody tr:nth-child(odd) {
        background: #f5f5f5;
    }

    .panel-df thead {
        border-bottom: 1px solid black;
        vertical-align: bottom;
    }

    .panel-df tr:hover {
        background: lightblue !important;
        cursor: pointer;
    }

'''

css2 = '''

table.panel-df {
        margin-left: auto;
        margin-right: auto;
        color: black;
        border: none;
        border-collapse: collapse;
        border-spacing: 0;
        font-size: 12px;
        table-layout: fixed;
        width: 100%;
    }

    .panel-df tr, .panel-df th, .panel-df td {
        text-align: right;
        vertical-align: middle;
        padding: 0.5em 0.5em !important;
        line-height: normal;
        white-space: normal;
        max-width: none;
        border: none;
    }

    .panel-df tbody {
        display: table-row-group;
        vertical-align: middle;
        border-color: inherit;
    }

    .panel-df tbody tr:nth-child(odd) {
        background: #f5f5f5;
    }

    .panel-df thead {
        border-bottom: 1px solid black;
        vertical-align: bottom;
    }

    .panel-df tr:hover {
        background: lightblue !important;
        cursor: pointer;
    }

'''

# used in pannel_process_revenue
css3 = '''
.panel-widget-box {
  background: #ffffff;
  border-radius: 5px;
  border: 1px black solid;
}
.panel-hover-tool {
  z-index: 10;
  padding-bottom: 15px;
}
.panel-hover-tool2 {
  z-index: 11;
}
.table-style {
width: 120%;
}
.table-style td {
width: 120%;
}

table.panel-df {
        margin-left: auto;
        margin-right: auto;
        color: black;
        border: none;
        border-collapse: collapse;
        border-spacing: 0;
        font-size: 12px;
        table-layout: auto;
        width: 120%;
    }

    .panel-df tr, .panel-df th, .panel-df td {
        text-align: right;
        vertical-align: middle;
        padding: 0.5em 0.5em !important;
        line-height: normal;
        white-space: normal;
        max-width: none;
        border: none;
        width: 15%;
    }

    .panel-df tbody {
        display: table-row-group;
        vertical-align: middle;
        border-color: inherit;
    }

    .panel-df tbody tr:nth-child(odd) {
        background: #f5f5f5;
    }

    .panel-df thead {
        border-bottom: 1px solid black;
        vertical-align: bottom;
    }

    .panel-df tr:hover {
        background: lightblue !important;
        cursor: pointer;
    }

table.panel-df2 {
        margin-left: auto;
        margin-right: auto;
        color: black;
        border: none;
        border-collapse: collapse;
        border-spacing: 0;
        font-size: 12px;
        table-layout: auto;
        width: 120%;
    }

    .panel-df2 tr, .panel-df2 th, .panel-df2 td {
        text-align: right;
        vertical-align: middle;
        padding: 0.5em 0.5em !important;
        line-height: normal;
        white-space: normal;
        max-width: none;
        border: none;
        width: 5%;
    }

    .panel-df2 tbody {
        display: table-row-group;
        vertical-align: middle;
        border-color: inherit;
    }

    .panel-df2 tbody tr:nth-child(odd) {
        background: #f5f5f5;
    }

    .panel-df2 thead {
        border-bottom: 1px solid black;
        vertical-align: bottom;
    }

    .panel-df2 tr:hover {
        background: lightblue !important;
        cursor: pointer;
    }


'''
