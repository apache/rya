<%@ page contentType="text/html; charset=iso-8859-1" language="java" %>
<%@ page import="java.net.*" %>
<%
    String sparql=request.getParameter("sparql");
    String endTime=request.getParameter("endTime");
    String startTime=request.getParameter("startTime");
    
    if(sparql != null){
        String sparqlEnc = URLEncoder.encode(sparql,"UTF-8");
        String urlTo = "queryrdf?binding.start="+startTime+"&binding.end="+endTime+"&query=" + sparqlEnc;
        response.sendRedirect(urlTo);
    }
%>
<html>
<body>
<form name="sparqlQuery" method="post" action="sparqlQuery.jsp">
<table width="100%" border="0" cellspacing="0" cellpadding="0">
  <tr>
    <td width="22%">&nbsp;</td>
    <td width="78%">&nbsp;</td>
    </tr>
  <tr>
    <td>SPARQL Query: </td>
    <td><textarea cols="200" rows="60" name="sparql">
Enter Sparql query here
    </textarea></td>
  </tr>
  <tr>
    <td>Start Time</td>
    <td><INPUT TYPE=TEXT NAME="startTime" SIZE="20"></td>
  </tr>
  <tr>
    <td>End Time</td>
    <td><INPUT TYPE=TEXT NAME="endTime" SIZE="20"></td>
  </tr>
  <tr>
    <td>&nbsp;</td>
    <td><input type="submit" name="submit" value="Submit"></td>
    </tr>
  <tr>
    <td>&nbsp;</td>
    <td>&nbsp;</td>
    </tr>
</table>
</form>
</body>
</html>