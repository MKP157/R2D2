mod database;
use http::{response, Request, Response, StatusCode, Version};
use std::io::{BufRead, Read, Write};
use std::net::{TcpListener, TcpStream};
use bson::{doc, Document};
use bson::spec::ElementType;
use crate::database::Database;

fn handle_client(mut stream: TcpStream) {
    println!("Connection from {}", stream.peer_addr().unwrap());
    let buf = &mut [0; 512];

    for line in stream.read(buf) {
        println!("{:?}", buf.to_vec());
    }
}


fn handle_request(_db: &mut Database, _req: String) -> Vec<u8> {
    // Query database and fetch result
    let result : Document = _db.query(_req.clone());

    let mut html : String = String::from(r#"
        <!DOCTYPE html>
        <html lang="en">
            <head>
                <meta charset="UTF-8">
                <link rel="icon" href="data:,">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>R2D2</title>
            </head>
            <body>
                <h1>"#);

    html.push_str(_req.as_str());
    html.push_str( r#"</h1></body><table>"#);

    // Header
    if let header = result.get_array("labels").unwrap() {
        let mut header_str : Vec<String> = Vec::from(
            header.iter()
                .filter(|&s| s.element_type() == ElementType::String)
                .map(|s| {String::from(s.as_str().unwrap())})
                .collect::<Vec<String>>()
        );

        if _req.as_str().contains("LIST") || _req.as_str().contains("INSERT") {
            header_str.insert(0, String::from("Timestamp"));
        }

        html.push_str(
            &*vec_string_to_html_row(header_str, true)
        );
    }

    // Contents
    if let mut body = result.get_document("rows").unwrap() {
        for (label, content) in body.iter() {
            match content.element_type() {
                ElementType::EmbeddedDocument => {
                    html.push_str(
                        &*document_to_html_row(
                            content.as_document().unwrap().clone(),
                            label.parse::<u128>().unwrap_or(0)
                        )
                    );
                }

                _ => {
                    html.push_str(format!(
                        "<tr><td>{}</td></tr>",
                        content.to_string()
                    ).as_str());
                }
            }


        }
    }

    html.push_str( r#"</table></html>"#);

    let mut header = String::new();
    header.push_str("HTTP/1.1 ");
    header.push_str(&200.to_string());
    header.push_str(" OK\r\n\r\n" );

    let mut result:Vec<u8> = Vec::from(header.as_bytes());
    result = [result, Vec::from(html.as_bytes())].concat();
    
    return result;
}

fn vec_string_to_html_row(v : Vec<String>, header : bool) -> String {
    let mut html = String::from("<tr>");
    for s in v {
        match header {
            true => html.push_str(format!("<th>{}</th>", s).as_str()),
            false => html.push_str(format!("<td>{}</td>", s).as_str()),
        };
    }

    html.push_str("</tr>");

    return html;
}

fn document_to_html_row(doc : Document, time : u128) -> String {
    let mut html = String::from(format!("<tr><td>{}</td>", time).as_str());

    for (label, content) in doc {
        // Don't want label! Ignore it
        let converted = match content.element_type() {
            ElementType::Double => content.as_f64().unwrap().to_string(),
            ElementType::Int32 => content.as_i32().unwrap().to_string(),
            ElementType::Int64 => content.as_i64().unwrap().to_string(),
            ElementType::String => content.as_str().unwrap().to_string(),
            ElementType::Boolean => content.as_bool().unwrap().to_string(),
            _ => String::from("None")
        };

        html.push_str(format!("<td>{}</td>", converted).as_str());
    }

    html.push_str("</tr>");

    return html;
}

fn main() -> std::io::Result<()> {
    let mut database = Database::new(
        vec![String::from("Name"), String::from("Cost"), String::from("Member")],
        vec![String::from("string"), String::from("number"), String::from("bool")]
    );

    database.insert_to_database(5, doc![
        "Name" : "Matthew",
        "Cost" : 100,
        "Member" : false,
    ]);

    database.insert_to_database(7, doc![
        "Name" : "Aiden",
        "Cost" : 65,
        "Member" : false,
    ]);

    database.insert_to_database(1, doc![
        "Name" : "Kim",
        "Cost" : 200,
        "Member" : true,
    ]);

    database.insert_to_database(35, doc![
        "Name" : "Bob",
        "Cost": 50,
        "Member" : true,
    ]);

    let listener = TcpListener::bind("127.0.0.1:6969")?;

    // accept connections and process them serially
    for mut stream in listener.incoming().flatten() {
        // Inside this loop, someone has connected.

        // You can kind of think of this line of code as if it were
        // a scanner in Java. That's basically what it's doing.
        let mut rdr = std::io::BufReader::new(&mut stream);

        /* [[[[[[[[[[[[[[ THE LISTEN LOOP ]]]]]]]]]]]]] */
        // This loop will get every string that the listener
        // hears, and print them to the terminal. If it hears
        // an empty line, we break out of the loop.
        let mut i = 0;
        let mut requested_resource: String = String::new();

        loop {
            let mut l = String::new();
            rdr.read_line(&mut l).unwrap();
            if l.trim().is_empty() { break; }

            if i == 0 {
                i = 1;
                requested_resource = l
                    .split(" ").collect::<Vec<&str>>()[1].to_string()
                    .split("/").collect::<Vec<&str>>()[1].to_string();
                println!("REQUESTED RESOURCE:{}", requested_resource);
            }

            print!("{l}");
        }

        let response = handle_request(&mut database, requested_resource);
        stream.write(&response).expect("TODO: panic message");
    }
    Ok(())
}