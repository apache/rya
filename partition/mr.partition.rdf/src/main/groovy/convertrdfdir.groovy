import org.openrdf.rio.rdfxml.*
import org.openrdf.rio.ntriples.NTriplesWriterFactory
import org.openrdf.rio.RDFHandler

@Grab(group='com.google.guava', module='guava', version='r06')
@Grab(group='org.openrdf.sesame', module='sesame-rio-rdfxml', version='2.3.2')
@Grab(group='org.openrdf.sesame', module='sesame-rio-ntriples', version='2.3.2')
@Grab(group='org.slf4j', module='slf4j-simple', version='1.5.8')
def convertDirRdfFormat(def dir, def outputFile) {
  //read each file
  assert dir.isDirectory()

  def ntriplesWriter = NTriplesWriterFactory.newInstance().getWriter(new FileOutputStream(outputFile))

  ntriplesWriter.startRDF()
  dir.listFiles().each { it ->
    //load file into rdfxml parser
    def rdfxmlParser = RDFXMLParserFactory.newInstance().getParser()
    rdfxmlParser.setRDFHandler(
        [       startRDF: {},
                endRDF: {},
                handleNamespace: { def prefix, def uri -> ntriplesWriter.handleNamespace(prefix, uri)},
                handleComment: {},
                handleStatement: { def stmt ->  ntriplesWriter.handleStatement stmt}] as RDFHandler
    )
    rdfxmlParser.parse(new FileInputStream(it), "")
  }
  ntriplesWriter.endRDF()
}

try{
convertDirRdfFormat(new File(args[0]), new File(args[1]))
}catch(Exception e) {e.printStackTrace();}