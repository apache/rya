<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE rdf:RDF [
   <!ENTITY xsd  "http://www.w3.org/2001/XMLSchema#" >
 ]>

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

<xsl:stylesheet version="1.0"
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
	xmlns:sparql="http://www.w3.org/2005/sparql-results#" xmlns="http://www.w3.org/1999/xhtml">

	<xsl:include href="../locale/messages.xsl" />

	<xsl:variable name="title">
		<xsl:value-of select="$repository-create.title" />
	</xsl:variable>

	<xsl:include href="template.xsl" />

	<xsl:template match="sparql:sparql">
		<form action="create">
			<table class="dataentry">
				<tbody>
					<tr>
						<th>
							<xsl:value-of select="$repository-type.label" />
						</th>
						<td>
							<select id="type" name="type">
								<option value="RyaAccumuloSail">
									Rya Accumulo Store
								</option>
								<option value="memory">
									In Memory Store
								</option>
								<option value="memory-rdfs">
									In Memory Store RDF Schema
								</option>
								<option value="memory-rdfs-dt">
									In Memory Store RDF Schema and
									Direct Type
									Hierarchy
								</option>
								<option value="memory-customrule">
									In Memory Store Custom Graph Query
									Inference
								</option>
								<option value="native">
									Native Java Store
								</option>
								<option value="native-rdfs">
									Native Java Store RDF Schema
								</option>
								<option value="native-rdfs-dt">
									Native Java Store RDF Schema and
									Direct Type
									Hierarchy
								</option>
								<option value="native-customrule">
									Native Java Store Custom
									Graph Query Inference
								</option>
								<option value="mysql">
									MySql RDF Store
								</option>
								<option value="pgsql">
									PostgreSQL RDF Store
								</option>
								<option value="remote">
									Remote RDF Store
								</option>
								<option value="sparql">
									SPARQL endpoint proxy
								</option>
								<option value="federate">Federation Store</option>
							</select>
						</td>
						<td></td>
					</tr>
					<tr>
						<th>
							<xsl:value-of select="$repository-id.label" />
						</th>
						<td>
							<input type="text" id="id" name="id" size="16" />
						</td>
						<td></td>
					</tr>
					<tr>
						<th>
							<xsl:value-of select="$repository-title.label" />
						</th>
						<td>
							<input type="text" id="title" name="title" size="48" />
						</td>
						<td></td>
					</tr>
					<tr>
						<td></td>
						<td>
							<input type="button" value="{$cancel.label}" style="float:right"
								href="repositories" onclick="document.location.href=this.getAttribute('href')" />
							<input type="submit" name="next" value="{$next.label}" />
						</td>
					</tr>
				</tbody>
			</table>
		</form>
	</xsl:template>

</xsl:stylesheet>
