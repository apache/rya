<%@ page contentType="text/html; charset=iso-8859-1" language="java" %>
<%@ page import="java.net.*" %>
<%
    String serql=request.getParameter("serql");
    if(serql != null){
        String serqlEnc = URLEncoder.encode(serql,"UTF-8");
        String urlTo = "queryserql?query=" + serqlEnc;
        response.sendRedirect(urlTo);
    }
%>
<html>
<body>
<form name="serqlQuery" method="post" action="serqlQuery.jsp">
<table width="100%" border="0" cellspacing="0" cellpadding="0">
  <tr>
    <td width="22%">&nbsp;</td>
    <td width="78%">&nbsp;</td>
    </tr>
  <tr>
    <td>SERQL Query: </td>
    <td><textarea cols="150" rows="40" name="serql">
Enter Serql query here
    </textarea></td>
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