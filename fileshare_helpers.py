# Author: K. Walsh <kwalsh@holycross.edu>
# Date: 15 October 2022

# Helper code for a cloud file storage service.
# Intended usage:
#   import fileshare_helpers
# or:
#   import fileshare_helpers as fh
# or:
#   from fileshare_helpers import *

# Given an integer size, in bytes, returns a pretty string.
# For example, pretty_size(3520500) returns "35.2 MB"
# And similarly, pretty_size(71030) returns "71.0 KB"
# Note: This function uses multiples of 1000, rather than multiples of 1024, so
# that 1 KB is exactly 1000 bytes, and 1 MB is exactly 1 million bytes, etc.
def pretty_size(n):
    gb = round(n / 1000000000, 2)
    mb = round(n / 1000000, 2)
    kb = round(n / 1000, 2)
    if gb >= 100:
        return "%.0f GB" % (gb)
    elif gb >= 10:
        return "%.1f GB" % (gb)
    elif gb >= 1:
        return "%.2f GB" % (gb)
    elif mb >= 100:
        return "%.0f MB" % (mb)
    elif mb >= 10:
        return "%.1f MB" % (mb)
    elif mb >= 1:
        return "%.2f MB" % (mb)
    elif kb >= 100:
        return "%.0f KB" % (kb)
    elif kb >= 10:
        return "%.1f KB" % (kb)
    elif kb >= 1:
        return "%.2f KB" % (kb)
    else:
        return "%d B" % (n)

def first_element_of_pair(elt):
    return elt[0]

def second_element_of_pair(elt):
    return elt[1]

# Given a list of (filename, size) pairs, this function returns a pretty HTML
# page containing that list, along with appropriate buttons for viewing,
# downloading, or deleting those files, or uploading new ones.
# The first two parameters, my_city and my_addr, are shown at the top of the
# page. The last parameter, extra_message, is optional. If given, it is also
# shown near the top of the page.
# Example:
#   my_city = "Worcester, MA"
#   my_addr = "1.2.3.4" # or "some-server.cloud.google.com"
#   listing = [ ("hello.txt", 415), ("example.mov", 150512), ("foo.pdf", 22500) ]
#   html = make_pretty_main_page(my_city, my_addr, listing)
# Alternatively:
#   filenames = [ "hello.txt", "example.mov", "foo.pdf" ]
#   filesizes = [ 415, 150512, 22500 ]
#   html = make_pretty_main_page(my_city, my_addr, list(zip(filenames, filesizes)) )
def make_pretty_main_page(my_city, my_addr, listing, extra_message=None):

    # Sort the listing alphabetically
    listing = sorted(listing, key = first_element_of_pair)

    upload_form = """
      <form id="upload-form" action="/upload" method="POST" enctype="multipart/form-data">
        <input id="upload-button" type="button" value="Upload New Files" />
        <input id="select-button" type="file" style="display: none;" name="files[]" multiple/>
        <input type="submit" style="display: none;" />
        <script>
          var upload = document.getElementById('upload-button');
          var select = document.getElementById('select-button');
          var form = document.getElementById('upload-form');
          upload.onclick = function() { select.click(); }
          select.onchange = function() { if (select.files.length > 0) form.submit(); }
        </script>
      </form>
    """

    #  # This is a simpler, but not as pretty, way to do the file upload form.
    #  upload_form = """
    #    <form action="/upload" method="POST" enctype="multipart/form-data">
    #      <input type="submit" value="Upload">
    #      <input type="file" name="files[]" />
    #    </form>
    #  """

    table_row_template = """
          <tr>
            <td>
              <form action="/delete/FILENAME" method="POST">
                <input type="submit" class="trash" value="&#xf1f8;" />
              </form>
            </td>
            <td><a href="/download/FILENAME" download><i class="fa fa-download"></a></td>
            <td><a href="/view/FILENAME">FILENAME</a></td>
            <td>FILESIZE</td>
          </tr>
    """

    # This is a slightly different way to do the delete operation. It uses
    # a POST to /delete with the filename as a form parameter
    # table_row_template = """
    #       <tr>
    #         <td>
    #           <form action="/delete" method="POST">
    #             <input type="text" name="filename" value="FILENAME" style="display: none;">
    #             <input type="submit" class="trash" value="&#xf1f8;" />
    #           </form>
    #         </td>
    #         <td><a href="/download/FILENAME" download><i class="fa fa-download"></a></td>
    #         <td><a href="/view/FILENAME">FILENAME</a></td>
    #         <td>FILESIZE</td>
    #       </tr>
    # """

    html = """
      <html>
      <head>
          <link rel="stylesheet" href="fileshare.css">
          <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css">
          <title>HC Cloud Drive</title>
      </head>
      <body>
    """

    html += "\n"
    html += "  <h1>HC Cloud Drive</h1>\n"
    html += "  <p>Current Server Location: " + my_city + "<br>Current Server Address: " + my_addr+"</p>\n"
    html += "\n"
    if extra_message is not None:
        html += "  <p><b>%s</b></p>\n" % (extra_message)
        html += "\n"
    html += "  <p>Below is a list of your %d cloud drive files.</p>\n" % (len(listing))
    html += "\n"
    html += upload_form
    html += "\n"
    html += "<table>\n"
    html += "<thead><tr><th></th><th></th><th>Name</th><th>Size</th></tr></thead>\n"
    html += "<tbody>\n"

    if len(listing) == 0:
        html += "\n"
        html += "<td></td><td></td><td><i>Sorry, you have no files. Try uploading?</i></td><td></td>\n"
        html += "\n"
    else:
        for name, size in listing:
            row = table_row_template
            row = row.replace("FILENAME", name)
            row = row.replace("FILESIZE", pretty_size(size))
            html += "\n"
            html += row
            html += "\n"

    html += """
        </table>
      
        <footer>
        System designed and implemented by kwalsh@holycross.edu<br>
        Go to <a href="/dashboard.html">system dashboard</a>.
        </footer>
      
      </body>
      </html>
    """

    return html
